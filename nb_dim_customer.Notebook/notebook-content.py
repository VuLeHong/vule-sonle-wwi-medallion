# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "233f4c6f-57c9-4c54-bea6-60a9126b4070",
# META       "default_lakehouse_name": "lh_vule_sonle_medallion",
# META       "default_lakehouse_workspace_id": "174d659d-2d6e-4d9f-86df-19eba8ef09a7",
# META       "known_lakehouses": [
# META         {
# META           "id": "233f4c6f-57c9-4c54-bea6-60a9126b4070"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# GOLD NOTEBOOK — DIM_CUSTOMER (SCD2)
# Source: Silver.Sales_Customers + Silver.Sales_CustomerCategories
# SCD2 logic:
#   - scd_from = audit_ts (thời điểm thay đổi trong Silver)
#   - Recalculate SCD2 chain: scd_to, scd_version, scd_active
#   - Xử lý inferred fill nếu có
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType
from delta.tables import DeltaTable
from datetime import datetime

# ============================================================
# 1. Hardcode parameters
# ============================================================

METADATA_DB = "lh_vule_sonle_medallion"

SOURCE_OBJECT = "Sales_Customers"
TARGET_OBJECT = "Dim_Customer"
TARGET_SCHEMA = "Gold"

CONFIG_TABLE = f"{METADATA_DB}.etl.config_gold_tables"
WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"

SKEY_COL = "customer_skey"
DEFAULT_SCD_TO = "2999-12-31 00:00:00"

execution_start_time = datetime.now()

# Flag để stop notebook thành công khi không có data mới
no_new_data = False

print("=" * 80)
print("[START] Gold SCD2 load for DIM_CUSTOMER")
print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
print(f"[SKEY_COL] {SKEY_COL}")
print(f"[START_TIME] {execution_start_time}")
print("=" * 80)


# ============================================================
# 2. Helper functions
# ============================================================

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def get_row_value(row, col_name, default=None):
    row_dict = row.asDict()
    return row_dict.get(col_name, default)


def split_config_list(value):
    if value is None:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def create_hash_expr(cols):
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                for c in cols
            ]
        ),
        256
    )


def get_max_skey(target_table: str, skey_col: str) -> int:
    if not table_exists(target_table):
        return 0

    result = (
        spark.table(target_table)
        .agg(F.max(F.col(skey_col)).alias("max_skey"))
        .collect()[0]["max_skey"]
    )

    return int(result or 0)


def add_surrogate_key(df, target_table: str, skey_col: str, business_keys: list):
    max_skey = get_max_skey(target_table, skey_col)

    order_cols = []

    for c in business_keys:
        if c in df.columns:
            order_cols.append(F.col(c).asc())

    order_cols += [
        F.col("audit_ts").asc(),
        F.col("hash_key").asc()
    ]

    w_skey = Window.orderBy(*order_cols)

    return df.withColumn(
        skey_col,
        F.lit(max_skey) + F.row_number().over(w_skey)
    )


def align_to_target_columns(df, target_table: str):
    if not table_exists(target_table):
        return df

    target_cols = spark.table(target_table).columns

    for c in target_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    return df.select(*target_cols)


def source_change_ts_expr():
    return F.greatest(
        F.to_timestamp(F.col("audit_ts")),
        F.coalesce(
            F.to_timestamp(F.col("deleted_audit_ts")),
            F.to_timestamp(F.col("audit_ts"))
        )
    )


# ============================================================
# 3. Load Gold config cho Sales_Customers (primary)
# ============================================================

try:
    config_row = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("source_object") == SOURCE_OBJECT) &
            (F.col("is_active") == 1)
        )
        .limit(1)
        .collect()
    )

    if not config_row:
        raise RuntimeError(
            f"[ERROR] Không tìm thấy config cho source_object = '{SOURCE_OBJECT}'"
        )

    metadata = config_row[0]

    source_schema = "Silver"
    source_object = get_row_value(metadata, "source_object", SOURCE_OBJECT)
    business_key = get_row_value(metadata, "business_key")
    column_list = get_row_value(metadata, "column_list")

    business_keys = split_config_list(business_key)
    configured_columns = split_config_list(column_list)

    if not business_keys:
        raise RuntimeError("[ERROR] business_key trong config_gold_tables đang trống")

    if not configured_columns:
        raise RuntimeError("[ERROR] column_list trong config_gold_tables đang trống")

    silver_table_primary = f"{METADATA_DB}.{source_schema}.{source_object}"
    gold_table = f"{METADATA_DB}.{TARGET_SCHEMA}.{TARGET_OBJECT}"

    print(f"[CONFIG] Primary source table : {silver_table_primary}")
    print(f"[CONFIG] Target table         : {gold_table}")
    print(f"[CONFIG] Business key         : {business_keys}")
    print(f"[CONFIG] SKey column          : {SKEY_COL}")

except Exception as e:
    print(f"[FAILED] Load Gold config failed: {str(e)}")
    raise


# ============================================================
# 4. Đọc config cho lookup table (Sales_CustomerCategories)
# ============================================================

try:
    lookup_config_row = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("source_object") == "Sales_CustomerCategories") &
            (F.col("is_active") == 1)
        )
        .limit(1)
        .collect()
    )

    if not lookup_config_row:
        raise RuntimeError("[ERROR] Không tìm thấy config cho Sales_CustomerCategories (lookup)")

    lookup_meta = lookup_config_row[0]

    lookup_business_key = get_row_value(lookup_meta, "business_key")
    lookup_column_list = get_row_value(lookup_meta, "column_list")

    lookup_business_keys = split_config_list(lookup_business_key)
    lookup_columns = split_config_list(lookup_column_list)

    silver_table_lookup = f"{METADATA_DB}.Silver.Sales_CustomerCategories"

    print(f"[CONFIG] Lookup table        : {silver_table_lookup}")
    print(f"[CONFIG] Lookup business key : {lookup_business_keys}")

except Exception as e:
    print(f"[FAILED] Load lookup config failed: {str(e)}")
    raise


# ============================================================
# 5. Load Gold watermark
# ============================================================

try:
    watermark_row = (
        spark.table(WATERMARK_TABLE)
        .filter(
            (F.col("layer") == "gold") &
            (F.col("object_name") == TARGET_OBJECT)
        )
        .orderBy(F.col("timestamp").desc())
        .limit(1)
        .select(F.to_timestamp("key_1").alias("max_audit_ts"))
        .collect()
    )

    gold_watermark_ts = watermark_row[0]["max_audit_ts"] if watermark_row else None

    print(f"[WATERMARK] Current Gold watermark: {gold_watermark_ts}")

except Exception as e:
    print(f"[FAILED] Load Gold watermark failed: {str(e)}")
    raise


# ============================================================
# 6. Read changed records from Silver Primary
#    Nếu không có data mới:
#    - Không raise SystemExit
#    - Set no_new_data = True
#    - Sau try block sẽ notebookutils.notebook.exit("NO_NEW_DATA")
# ============================================================

try:
    if not table_exists(silver_table_primary):
        raise RuntimeError(f"[ERROR] Source Silver table không tồn tại: {silver_table_primary}")

    if not table_exists(silver_table_lookup):
        raise RuntimeError(f"[ERROR] Lookup Silver table không tồn tại: {silver_table_lookup}")

    df_primary = spark.table(silver_table_primary)

    if "audit_ts" not in df_primary.columns:
        raise RuntimeError("[ERROR] Primary Silver table không có audit_ts để chạy watermark Gold")

    if "deleted_audit_ts" not in df_primary.columns:
        df_primary = df_primary.withColumn(
            "deleted_audit_ts",
            F.lit(None).cast(TimestampType())
        )

    if gold_watermark_ts is None:
        df_changed_primary = df_primary
    else:
        df_changed_primary = df_primary.filter(
            source_change_ts_expr() > F.lit(gold_watermark_ts)
        )

    if df_changed_primary.isEmpty():
        no_new_data = True

        execution_end_time = datetime.now()

        print("=" * 80)
        print("[SUCCESS] Gold SCD2 load completed - No new data")
        print("[STOP] No new data")
        print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
        print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
        print(f"[START_TIME] {execution_start_time}")
        print(f"[END_TIME] {execution_end_time}")
        print(f"[DURATION] {execution_end_time - execution_start_time}")
        print("=" * 80)

    else:
        max_source_audit_ts = (
            df_changed_primary
            .agg(F.max(source_change_ts_expr()).alias("max_ts"))
            .collect()[0]["max_ts"]
        )

        changed_primary_count = df_changed_primary.count()

        print(f"[INFO] Changed records from Silver: {changed_primary_count}")
        print(f"[INFO] Max source audit/deleted timestamp from Silver: {max_source_audit_ts}")

except Exception as e:
    print(f"[FAILED] Read changed Silver records failed: {str(e)}")
    raise


# ============================================================
# Stop notebook successfully if no new data
# IMPORTANT:
#   - Không dùng raise SystemExit
#   - Không đặt notebookutils.notebook.exit trong try/except
# ============================================================

if no_new_data:
    notebookutils.notebook.exit("NO_NEW_DATA")


# ============================================================
# 7. Enrich changed records with lookup CustomerCategoryName
# ============================================================

try:
    df_lookup = spark.table(silver_table_lookup)

    if "audit_ts" not in df_lookup.columns:
        raise RuntimeError("[ERROR] Lookup Silver table không có audit_ts")

    if "deleted_audit_ts" not in df_lookup.columns:
        df_lookup = df_lookup.withColumn(
            "deleted_audit_ts",
            F.lit(None).cast(TimestampType())
        )

    w_lookup = Window.partitionBy("hash_key").orderBy(F.col("audit_ts").desc())

    df_lookup_current = (
        df_lookup
        .withColumn("_rn", F.row_number().over(w_lookup))
        .filter(F.col("_rn") == 1)
        .filter(F.col("deleted_audit_ts").isNull())
        .drop("_rn")
        .select(
            F.col("CustomerCategoryID").alias("lookup_CustomerCategoryID"),
            F.col("CustomerCategoryName")
        )
    )

    df_enriched = (
        df_changed_primary
        .join(
            F.broadcast(df_lookup_current),
            df_changed_primary["CustomerCategoryID"] == df_lookup_current["lookup_CustomerCategoryID"],
            how="left"
        )
        .withColumn(
            "CustomerCategoryName",
            F.coalesce(F.col("CustomerCategoryName"), F.lit("UNKNOWN"))
        )
        .drop("lookup_CustomerCategoryID")
    )

    print("[INFO] Enriched changed records with CustomerCategoryName")

except Exception as e:
    print(f"[FAILED] Enrich with lookup failed: {str(e)}")
    raise


# ============================================================
# 8. Prepare staged data
# ============================================================

try:
    selected_cols = list(dict.fromkeys(
        business_keys + configured_columns + ["audit_ts"]
    ))

    if "source_id" in df_enriched.columns and "source_id" not in selected_cols:
        selected_cols.append("source_id")

    if "deleted_audit_ts" in df_enriched.columns and "deleted_audit_ts" not in selected_cols:
        selected_cols.append("deleted_audit_ts")

    df_staged_raw = df_enriched.select(*selected_cols)

    df_staged_raw = df_staged_raw.withColumn(
        "hash_key",
        create_hash_expr(business_keys)
    )

    exclude_for_row_hash = set(business_keys) | {
        "audit_ts",
        "source_id",
        "deleted_audit_ts",
        "hash_key",
        "row_hash"
    }

    hash_columns = [
        c for c in configured_columns
        if c in df_staged_raw.columns and c not in exclude_for_row_hash
    ]

    if not hash_columns:
        raise RuntimeError("[ERROR] Không có business attribute nào để tạo row_hash")

    df_staged_raw = df_staged_raw.withColumn(
        "row_hash",
        create_hash_expr(hash_columns)
    )

    w_latest = Window.partitionBy("hash_key").orderBy(
        source_change_ts_expr().desc(),
        F.col("audit_ts").desc(),
        F.col("deleted_audit_ts").desc_nulls_last()
    )

    df_staged = (
        df_staged_raw
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .cache()
    )

    staged_count = df_staged.count()

    print(f"[STAGED] Prepared staged records: {staged_count}")
    print(f"[STAGED] Row hash columns: {hash_columns}")

except Exception as e:
    print(f"[FAILED] Prepare staged data failed: {str(e)}")
    raise


# ============================================================
# 9. Create Gold schema if needed
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.{TARGET_SCHEMA}")
    print("[INFO] Gold schema checked")

except Exception as e:
    print(f"[FAILED] Create Gold schema failed: {str(e)}")
    raise


# ============================================================
# 10. Fill inferred dimension records
# ============================================================

try:
    inferred_filled_rows = 0

    if not table_exists(gold_table):
        print("[INFERRED] Target table chưa tồn tại, skip step")
        df_staged_after_inferred = df_staged

    else:
        df_gold = spark.table(gold_table)

        required_cols = {
            "hash_key",
            SKEY_COL,
            "scd_active",
            "inferred_flag"
        }

        missing = [c for c in required_cols if c not in df_gold.columns]

        if missing:
            raise RuntimeError(f"[ERROR] Gold dimension thiếu required columns: {missing}")

        df_gold_active_inferred = (
            df_gold
            .filter(
                (F.col("scd_active") == 1) &
                (F.col("inferred_flag") == 1)
            )
            .select("hash_key", SKEY_COL)
        )

        df_inferred_to_fill = (
            df_staged
            .filter(F.col("deleted_audit_ts").isNull())
            .join(df_gold_active_inferred, on="hash_key", how="inner")
            .cache()
        )

        inferred_filled_rows = df_inferred_to_fill.count()

        if inferred_filled_rows > 0:
            print(f"[INFERRED] Fill inferred rows: {inferred_filled_rows}")

            update_set = {
                "audit_ts": "s.audit_ts",
                "updated_audit_ts": "current_timestamp()",
                "deleted_audit_ts": "s.deleted_audit_ts",
                "row_hash": "s.row_hash",
                "inferred_flag": "0"
            }

            for c in configured_columns:
                if c in df_inferred_to_fill.columns and c in df_gold.columns:
                    update_set[c] = f"s.`{c}`"

            if "CustomerCategoryName" in df_inferred_to_fill.columns and "CustomerCategoryName" in df_gold.columns:
                update_set["CustomerCategoryName"] = "s.CustomerCategoryName"

            DeltaTable.forName(spark, gold_table).alias("t") \
                .merge(
                    df_inferred_to_fill.alias("s"),
                    f"t.{SKEY_COL} = s.{SKEY_COL}"
                ) \
                .whenMatchedUpdate(set=update_set) \
                .execute()

            filled_keys = df_inferred_to_fill.select("hash_key").distinct()

            df_staged_after_inferred = (
                df_staged
                .join(filled_keys, on="hash_key", how="left_anti")
                .cache()
            )

        else:
            print("[INFERRED] No inferred rows to fill")
            df_staged_after_inferred = df_staged

        df_inferred_to_fill.unpersist()

except Exception as e:
    print(f"[FAILED] Fill inferred dimension failed: {str(e)}")
    raise


# ============================================================
# 11. Phân loại new / changed / deleted
# ============================================================

try:
    df_non_deleted_staged = (
        df_staged_after_inferred
        .filter(F.col("deleted_audit_ts").isNull())
        .cache()
    )

    df_deleted_staged = (
        df_staged_after_inferred
        .filter(F.col("deleted_audit_ts").isNotNull())
        .cache()
    )

    if table_exists(gold_table):
        df_gold_active = (
            spark.table(gold_table)
            .filter(F.col("scd_active") == 1)
            .select(
                "hash_key",
                SKEY_COL,
                F.col("row_hash").alias("_gold_row_hash")
            )
            .cache()
        )

        df_new = (
            df_non_deleted_staged
            .join(
                df_gold_active.select("hash_key"),
                on="hash_key",
                how="left_anti"
            )
            .cache()
        )

        df_changed = (
            df_non_deleted_staged
            .join(df_gold_active, on="hash_key", how="inner")
            .filter(F.col("row_hash") != F.col("_gold_row_hash"))
            .drop("_gold_row_hash", SKEY_COL)
            .cache()
        )

        df_deleted = (
            df_deleted_staged
            .join(
                df_gold_active.select("hash_key", SKEY_COL),
                on="hash_key",
                how="inner"
            )
            .cache()
        )

    else:
        df_new = df_non_deleted_staged.cache()
        df_changed = spark.createDataFrame([], df_non_deleted_staged.schema)
        df_deleted = spark.createDataFrame([], df_deleted_staged.schema)

    new_rows = df_new.count()
    changed_rows = df_changed.count()
    deleted_rows = df_deleted.count()

    print(f"[CHECK] New records      : {new_rows}")
    print(f"[CHECK] Changed records  : {changed_rows}")
    print(f"[CHECK] Deleted records  : {deleted_rows}")
    print(f"[CHECK] Inferred filled  : {inferred_filled_rows}")

except Exception as e:
    print(f"[FAILED] Classify new/changed/deleted failed: {str(e)}")
    raise


# ============================================================
# 12. Build SCD2 insert dataframe
# ============================================================

try:
    def build_scd2_insert_df(df_input):
        return (
            df_input
            .withColumn("updated_audit_ts", F.lit(None).cast(TimestampType()))
            .withColumn("scd_start", F.current_timestamp())
            .withColumn("scd_from", F.to_timestamp(F.col("audit_ts")))
            .withColumn("scd_to", F.to_timestamp(F.lit(DEFAULT_SCD_TO)))
            .withColumn("scd_active", F.lit(1).cast(IntegerType()))
            .withColumn("scd_version", F.lit(1).cast(IntegerType()))
            .withColumn("inferred_flag", F.lit(0).cast(IntegerType()))
        )

    df_new_insert = build_scd2_insert_df(df_new)
    df_changed_insert = build_scd2_insert_df(df_changed)

    print(f"[INSERT NEW] Prepared rows: {new_rows}")
    print(f"[INSERT CHANGED] Prepared rows: {changed_rows}")

except Exception as e:
    print(f"[FAILED] Build SCD2 insert dataframe failed: {str(e)}")
    raise


# ============================================================
# 13. Insert new and changed records
# ============================================================

try:
    df_to_insert = df_new_insert.unionByName(
        df_changed_insert,
        allowMissingColumns=True
    )

    insert_rows = df_to_insert.count()

    if insert_rows > 0:
        df_to_insert = add_surrogate_key(
            df_to_insert,
            gold_table,
            SKEY_COL,
            business_keys
        )

        ordered_cols = [SKEY_COL] + [
            c for c in df_to_insert.columns
            if c != SKEY_COL
        ]

        df_to_insert = df_to_insert.select(*ordered_cols)

        if table_exists(gold_table):
            df_to_insert = align_to_target_columns(df_to_insert, gold_table)

            df_to_insert.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(gold_table)

        else:
            df_to_insert.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(gold_table)

        print(f"[INSERT SCD2] Inserted rows: {insert_rows}")

    else:
        print("[INSERT SCD2] No new or changed records to insert")

except Exception as e:
    print(f"[FAILED] Insert SCD2 records failed: {str(e)}")
    raise


# ============================================================
# 14. Handle soft delete
# ============================================================

try:
    if deleted_rows > 0 and table_exists(gold_table):
        df_deleted_update = (
            df_deleted
            .select(
                SKEY_COL,
                F.col("deleted_audit_ts").alias("_deleted_audit_ts")
            )
            .dropDuplicates([SKEY_COL])
        )

        DeltaTable.forName(spark, gold_table).alias("t") \
            .merge(
                df_deleted_update.alias("s"),
                f"t.{SKEY_COL} = s.{SKEY_COL}"
            ) \
            .whenMatchedUpdate(set={
                "deleted_audit_ts": "s._deleted_audit_ts",
                "updated_audit_ts": "current_timestamp()"
            }) \
            .execute()

        print(f"[DELETE] Marked deleted active rows: {deleted_rows}")

    else:
        print("[DELETE] No deleted records to process")

except Exception as e:
    print(f"[FAILED] Handle soft delete failed: {str(e)}")
    raise


# ============================================================
# 15. Recalculate SCD2 columns
# ============================================================

try:
    affected_keys_df = (
        df_new.select("hash_key")
        .unionByName(df_changed.select("hash_key"))
        .unionByName(df_deleted.select("hash_key"))
        .distinct()
        .cache()
    )

    affected_key_count = affected_keys_df.count()

    if affected_key_count > 0 and table_exists(gold_table):
        print(f"[RECALC] Affected business keys: {affected_key_count}")

        df_target_affected = (
            spark.table(gold_table)
            .join(F.broadcast(affected_keys_df), on="hash_key", how="inner")
        )

        w_order = Window.partitionBy("hash_key").orderBy(
            F.col("scd_from").asc(),
            F.col("audit_ts").asc(),
            F.col(SKEY_COL).asc()
        )

        w_part = Window.partitionBy("hash_key")

        df_recalc = (
            df_target_affected
            .withColumn("_rn", F.row_number().over(w_order))
            .withColumn("_next_scd_from", F.lead("scd_from").over(w_order))
            .withColumn("_min_scd_start", F.min("scd_start").over(w_part))
            .withColumn(
                "_new_scd_to",
                F.when(
                    F.col("deleted_audit_ts").isNotNull(),
                    F.col("deleted_audit_ts")
                )
                .when(
                    F.col("_next_scd_from").isNotNull(),
                    F.col("_next_scd_from")
                )
                .otherwise(F.to_timestamp(F.lit(DEFAULT_SCD_TO)))
            )
            
            .withColumn(
                "_new_scd_active",
                F.when(
                    F.col("deleted_audit_ts").isNotNull(),
                    F.lit(0)
                )
                .when(
                    F.col("_next_scd_from").isNull(),
                    F.lit(1)
                )
                .otherwise(F.lit(0))
            )
            .select(
                SKEY_COL,
                F.col("_min_scd_start").alias("new_scd_start"),
                F.col("scd_from").alias("new_scd_from"),
                F.col("_new_scd_to").alias("new_scd_to"),
                F.col("_new_scd_active").cast(IntegerType()).alias("new_scd_active"),
                F.col("_rn").cast(IntegerType()).alias("new_scd_version")
            )
        )

        DeltaTable.forName(spark, gold_table).alias("t") \
            .merge(
                df_recalc.alias("s"),
                f"t.{SKEY_COL} = s.{SKEY_COL}"
            ) \
            .whenMatchedUpdate(set={
                "scd_start": "s.new_scd_start",
                "scd_from": "s.new_scd_from",
                "scd_to": "s.new_scd_to",
                "scd_active": "s.new_scd_active",
                "scd_version": "s.new_scd_version"
            }) \
            .execute()

        print("[RECALC] SCD2 columns recalculated")

    else:
        print("[RECALC] No affected keys")

except Exception as e:
    print(f"[FAILED] Recalculate SCD2 columns failed: {str(e)}")
    raise


# ============================================================
# 16. Update Gold watermark
# ============================================================

try:
    watermark_new_df = (
        spark.createDataFrame(
            [("gold", TARGET_OBJECT, str(max_source_audit_ts), "datetime")],
            ["layer", "object_name", "key_1", "key_1_desc"]
        )
        .withColumn("timestamp", F.current_timestamp())
    )

    watermark_new_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(WATERMARK_TABLE)

    print(f"[WATERMARK] Updated Gold watermark: {max_source_audit_ts}")

except Exception as e:
    print(f"[FAILED] Update Gold watermark failed: {str(e)}")
    raise


# ============================================================
# 17. Summary and cleanup
# ============================================================

try:
    execution_end_time = datetime.now()
    total_updated_rows = changed_rows + inferred_filled_rows

    print("=" * 80)
    print("[SUCCESS] Gold SCD2 load completed")
    print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
    print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
    print(f"[START_TIME] {execution_start_time}")
    print(f"[END_TIME] {execution_end_time}")
    print(f"[DURATION] {execution_end_time - execution_start_time}")
    print("-" * 80)
    print(f"[SUMMARY] New rows          : {new_rows}")
    print(f"[SUMMARY] Changed rows      : {changed_rows}")
    print(f"[SUMMARY] Inferred filled   : {inferred_filled_rows}")
    print(f"[SUMMARY] Updated rows      : {total_updated_rows}")
    print(f"[SUMMARY] Deleted rows      : {deleted_rows}")
    print(f"[SUMMARY] Watermark updated : {max_source_audit_ts}")
    print("=" * 80)

finally:
    for df_name in [
        "df_staged",
        "df_staged_after_inferred",
        "df_non_deleted_staged",
        "df_deleted_staged",
        "df_new",
        "df_changed",
        "df_deleted",
        "affected_keys_df"
    ]:
        if df_name in locals():
            try:
                locals()[df_name].unpersist()
            except:
                pass

    if "df_gold_active" in locals():
        try:
            df_gold_active.unpersist()
        except:
            pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
