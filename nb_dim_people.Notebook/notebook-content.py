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
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# DEDICATED GOLD NOTEBOOK — DIM_PEOPLE (SCD1)
# Source: Silver.Application_People
# Target: Gold.Dim_People
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType
from delta.tables import DeltaTable
from datetime import datetime

# ============================================================
# 1. Hardcode
# ============================================================

METADATA_DB = "lh_vule_sonle_medallion"

SOURCE_OBJECT = "Application_People"
TARGET_OBJECT = "Dim_People"

CONFIG_TABLE = f"{METADATA_DB}.etl.config_gold_tables"
WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"

TARGET_SCHEMA = "Gold"

SKEY_COL = "person_skey"

DEFAULT_SCD_TO = "2999-12-31 00:00:00"

execution_start_time = datetime.now()

# Flag dùng để exit notebook thành công khi không có new data
no_new_data = False

print("=" * 80)
print("[START] Gold SCD1 load started")
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
# 3. Load Gold config
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

    silver_table = f"{METADATA_DB}.{source_schema}.{source_object}"
    gold_table = f"{METADATA_DB}.{TARGET_SCHEMA}.{TARGET_OBJECT}"

    print(f"[CONFIG] Source table : {silver_table}")
    print(f"[CONFIG] Target table : {gold_table}")
    print(f"[CONFIG] Business key : {business_keys}")
    print(f"[CONFIG] SKey column  : {SKEY_COL}")

except Exception as e:
    print(f"[FAILED] Load Gold config failed: {str(e)}")
    raise


# ============================================================
# 4. Load Gold watermark
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
# 5. Read changed records from Silver
#    Nếu không có data mới:
#    - Không raise SystemExit
#    - Set no_new_data = True
#    - Sau try block sẽ notebookutils.notebook.exit("NO_NEW_DATA")
# ============================================================

try:
    if not table_exists(silver_table):
        raise RuntimeError(f"[ERROR] Source Silver table không tồn tại: {silver_table}")

    df_silver = spark.table(silver_table)

    if "audit_ts" not in df_silver.columns:
        raise RuntimeError("[ERROR] Silver table không có audit_ts để chạy watermark Gold")

    if gold_watermark_ts is None:
        df_changed_silver = df_silver
    else:
        df_changed_silver = df_silver.filter(
            source_change_ts_expr() > F.lit(gold_watermark_ts)
        )

    if df_changed_silver.isEmpty():
        no_new_data = True

        execution_end_time = datetime.now()

        print("=" * 80)
        print("[SUCCESS] Gold SCD1 load completed - No new data")
        print("[STOP] No new data")
        print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
        print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
        print(f"[START_TIME] {execution_start_time}")
        print(f"[END_TIME] {execution_end_time}")
        print(f"[DURATION] {execution_end_time - execution_start_time}")
        print("=" * 80)

    else:
        max_source_audit_ts = (
            df_changed_silver
            .agg(F.max(source_change_ts_expr()).alias("max_ts"))
            .collect()[0]["max_ts"]
        )

        print(f"[INFO] Max source audit/deleted timestamp from Silver: {max_source_audit_ts}")

except Exception as e:
    print(f"[FAILED] Read changed Silver records failed: {str(e)}")
    raise


# ============================================================
# Stop notebook successfully if no new data
# IMPORTANT:
#   - Không đặt notebookutils.notebook.exit trong try/except
#   - Không dùng raise SystemExit
# ============================================================

if no_new_data:
    notebookutils.notebook.exit("NO_NEW_DATA")


# ============================================================
# 6. Prepare staged data
# ============================================================

try:
    selected_cols = list(dict.fromkeys(
        business_keys + configured_columns + ["audit_ts"]
    ))

    if "source_id" in df_changed_silver.columns and "source_id" not in selected_cols:
        selected_cols.append("source_id")

    if "deleted_audit_ts" in df_changed_silver.columns and "deleted_audit_ts" not in selected_cols:
        selected_cols.append("deleted_audit_ts")

    df_staged_raw = df_changed_silver.select(*selected_cols)

    df_staged_raw = df_staged_raw.withColumn(
        "deleted_audit_ts",
        F.to_timestamp(F.col("deleted_audit_ts"))
    )

    df_staged_raw = df_staged_raw.withColumn(
        "hash_key",
        create_hash_expr(business_keys)
    )

    exclude_for_row_hash = set(business_keys) | {
        "audit_ts",
        "source_id",
        "deleted_audit_ts",
        "hash_key",
        "row_hash",
    }

    hash_columns = [
        c for c in selected_cols
        if c in df_staged_raw.columns and c not in exclude_for_row_hash
    ]

    if not hash_columns:
        raise RuntimeError("[ERROR] Không có business attribute nào để tạo row_hash")

    df_staged_raw = df_staged_raw.withColumn(
        "row_hash",
        create_hash_expr(hash_columns)
    )

    w_latest = Window.partitionBy("hash_key").orderBy(
        F.greatest(
            F.to_timestamp(F.col("audit_ts")),
            F.coalesce(
                F.to_timestamp(F.col("deleted_audit_ts")),
                F.to_timestamp(F.col("audit_ts"))
            )
        ).desc(),
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
# 7. Create Gold schema if needed
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.{TARGET_SCHEMA}")
    print("[INFO] Gold schema checked")

except Exception as e:
    print(f"[FAILED] Create Gold schema failed: {str(e)}")
    raise


# ============================================================
# 8. Check new / changed / deleted records
#    SCD1: inferred records are handled as changed records
# ============================================================

try:
    df_non_deleted_staged = (
        df_staged
        .filter(F.col("deleted_audit_ts").isNull())
        .cache()
    )

    df_deleted_staged = (
        df_staged
        .filter(F.col("deleted_audit_ts").isNotNull())
        .cache()
    )

    if table_exists(gold_table):
        df_gold = spark.table(gold_table)

        required_cols = {
            "hash_key",
            SKEY_COL,
            "row_hash",
            "deleted_audit_ts",
            "inferred_flag"
        }

        missing_cols = [c for c in required_cols if c not in df_gold.columns]

        if missing_cols:
            raise RuntimeError(
                f"[ERROR] Gold dimension thiếu required columns: {missing_cols}"
            )

        df_gold_current = (
            df_gold
            .select(
                "hash_key",
                SKEY_COL,
                F.col("row_hash").alias("_gold_row_hash"),
                F.col("deleted_audit_ts").alias("_gold_deleted_audit_ts"),
                F.col("inferred_flag").alias("_gold_inferred_flag")
            )
            .cache()
        )

        df_new = (
            df_non_deleted_staged
            .join(
                df_gold_current.select("hash_key"),
                on="hash_key",
                how="left_anti"
            )
            .cache()
        )

        df_changed = (
            df_non_deleted_staged
            .join(df_gold_current, on="hash_key", how="inner")
            .filter(
                (F.col("row_hash") != F.col("_gold_row_hash")) |
                (F.col("_gold_deleted_audit_ts").isNotNull()) |
                (F.col("_gold_inferred_flag") == 1)
            )
            .drop(
                "_gold_row_hash",
                "_gold_deleted_audit_ts",
                "_gold_inferred_flag"
            )
            .cache()
        )

        df_deleted = (
            df_deleted_staged
            .join(
                df_gold_current.select("hash_key", SKEY_COL),
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

except Exception as e:
    print(f"[FAILED] Check new / changed / deleted failed: {str(e)}")
    raise


# ============================================================
# 9. Build SCD1 insert dataframe
# ============================================================

try:
    df_new_insert = (
        df_new
        .withColumn("updated_audit_ts", F.lit(None).cast(TimestampType()))
        .withColumn("scd_start", F.current_timestamp())
        .withColumn("scd_active", F.lit(1).cast(IntegerType()))
        .withColumn("scd_version", F.lit(1).cast(IntegerType()))
        .withColumn("inferred_flag", F.lit(0).cast(IntegerType()))
    )

    print(f"[INSERT NEW] Prepared rows: {new_rows}")

except Exception as e:
    print(f"[FAILED] Build SCD1 insert dataframe failed: {str(e)}")
    raise


# ============================================================
# 10. Insert new records
# ============================================================

try:
    if new_rows > 0:
        df_new_insert = add_surrogate_key(
            df_new_insert,
            gold_table,
            SKEY_COL,
            business_keys
        )

        ordered_cols = [SKEY_COL] + [
            c for c in df_new_insert.columns
            if c != SKEY_COL
        ]

        df_new_insert = df_new_insert.select(*ordered_cols)

        if table_exists(gold_table):
            df_new_insert = align_to_target_columns(df_new_insert, gold_table)

            df_new_insert.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(gold_table)

        else:
            df_new_insert.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(gold_table)

        print(f"[INSERT SCD1] Inserted new rows: {new_rows}")

    else:
        print("[INSERT SCD1] No new records to insert")

except Exception as e:
    print(f"[FAILED] Insert SCD1 new records failed: {str(e)}")
    raise


# ============================================================
# 11. Update changed records directly
#     SCD1 overwrite:
#     - changed attribute
#     - restored soft-deleted record
#     - inferred_flag = 1 record
# ============================================================

try:
    if changed_rows > 0 and table_exists(gold_table):
        df_changed_update = (
            df_changed
            .dropDuplicates(["hash_key"])
            .cache()
        )

        df_gold = spark.table(gold_table)

        update_set = {
            "audit_ts": "s.audit_ts",
            "updated_audit_ts": "current_timestamp()",
            "deleted_audit_ts": "s.deleted_audit_ts",
            "row_hash": "s.row_hash",
            "inferred_flag": "0",
            "scd_active": "1",
            "scd_version": "1"
        }

        if "source_id" in df_changed_update.columns and "source_id" in df_gold.columns:
            update_set["source_id"] = "s.source_id"

        for c in configured_columns:
            if c in df_changed_update.columns and c in df_gold.columns:
                update_set[c] = f"s.`{c}`"

        DeltaTable.forName(spark, gold_table).alias("t") \
            .merge(
                df_changed_update.alias("s"),
                "t.hash_key = s.hash_key"
            ) \
            .whenMatchedUpdate(set=update_set) \
            .execute()

        print(f"[UPDATE SCD1] Updated changed rows: {changed_rows}")

        df_changed_update.unpersist()

    else:
        print("[UPDATE SCD1] No changed records to update")

except Exception as e:
    print(f"[FAILED] Update SCD1 changed records failed: {str(e)}")
    raise


# ============================================================
# 12. Handle deleted records from Silver tombstone
# ============================================================

try:
    if deleted_rows > 0 and table_exists(gold_table):
        df_deleted_update = (
            df_deleted
            .select(
                "hash_key",
                F.col("deleted_audit_ts").alias("_deleted_audit_ts")
            )
            .dropDuplicates(["hash_key"])
        )

        DeltaTable.forName(spark, gold_table).alias("t") \
            .merge(
                df_deleted_update.alias("s"),
                "t.hash_key = s.hash_key"
            ) \
            .whenMatchedUpdate(set={
                "deleted_audit_ts": "s._deleted_audit_ts",
                "updated_audit_ts": "current_timestamp()"
            }) \
            .execute()

        print(f"[DELETE] Marked deleted rows: {deleted_rows}")

    else:
        print("[DELETE] No deleted records to process")

except Exception as e:
    print(f"[FAILED] Handle soft delete failed: {str(e)}")
    raise


# ============================================================
# 13. Update Gold watermark
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
# 14. End log and cleanup
# ============================================================

try:
    execution_end_time = datetime.now()

    print("=" * 80)
    print("[SUCCESS] Gold SCD1 load completed")
    print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
    print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
    print(f"[START_TIME] {execution_start_time}")
    print(f"[END_TIME] {execution_end_time}")
    print(f"[DURATION] {execution_end_time - execution_start_time}")
    print("-" * 80)
    print(f"[SUMMARY] New rows          : {new_rows}")
    print(f"[SUMMARY] Changed rows      : {changed_rows}")
    print(f"[SUMMARY] Updated rows      : {changed_rows}")
    print(f"[SUMMARY] Deleted rows      : {deleted_rows}")
    print(f"[SUMMARY] Watermark updated : {max_source_audit_ts}")
    print("=" * 80)

finally:
    if "df_staged" in locals():
        df_staged.unpersist()
    if "df_non_deleted_staged" in locals():
        df_non_deleted_staged.unpersist()
    if "df_deleted_staged" in locals():
        df_deleted_staged.unpersist()
    if "df_new" in locals():
        df_new.unpersist()
    if "df_changed" in locals():
        df_changed.unpersist()
    if "df_deleted" in locals():
        df_deleted.unpersist()
    if "df_gold_current" in locals():
        df_gold_current.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
