# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================
# DEDICATED GOLD NOTEBOOK — DIM_STOCKITEMS (SCD2)
# Source: Silver.Warehouse_StockItems
# Target: Gold.Dim_StockItems
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType, IntegerType
from delta.tables import DeltaTable
import uuid

# ============================================================
# 1. Hardcode
# ============================================================

METADATA_DB = "lh_vule_sonle_medallion"

SOURCE_OBJECT = "Warehouse_StockItems"
TARGET_OBJECT = "Dim_StockItems"

CONFIG_TABLE = f"{METADATA_DB}.etl.config_gold_tables"
WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"

DEFAULT_SCD_FROM = "1900-01-01 00:00:00"
DEFAULT_SCD_TO = "2999-12-31 00:00:00"

SKEY_COL = "stockitem_skey"

print(f"[INFO] Start Gold SCD2 notebook for {TARGET_OBJECT}")

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
            *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in cols]
        ),
        256
    )

def get_max_skey(target_table: str, skey_col: str) -> int:
    if not table_exists(target_table):
        return 0

    result = spark.table(target_table).agg(F.max(F.col(skey_col)).alias("max_skey")).collect()[0]["max_skey"]
    return int(result or 0)

def add_surrogate_key(df, target_table: str, skey_col: str):
    max_skey = get_max_skey(target_table, skey_col)

    w_skey = Window.orderBy(
        F.col("hash_key").asc(),
        F.col("scd_from").asc(),
        F.col("audit_ts").asc()
    )

    return df.withColumn(skey_col, F.lit(max_skey) + F.row_number().over(w_skey))

def align_to_target_columns(df, target_table: str):
    if not table_exists(target_table):
        return df

    target_cols = spark.table(target_table).columns

    for c in target_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    return df.select(*target_cols)

# ============================================================
# 3. Load Gold config
# ============================================================

try:
    config_row = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("target_object") == TARGET_OBJECT) &
            (F.col("is_active") == 1)
        )
        .limit(1)
        .collect()
    )

    if not config_row:
        raise RuntimeError(f"[ERROR] Không tìm thấy config Gold cho target_object = '{TARGET_OBJECT}'")

    metadata = config_row[0]

    source_schema = get_row_value(metadata, "source_schema", "silver")
    target_schema = get_row_value(metadata, "target_schema", "gold")
    source_object = get_row_value(metadata, "source_object", SOURCE_OBJECT)
    target_object = get_row_value(metadata, "target_object", TARGET_OBJECT)

    business_key = get_row_value(metadata, "business_key")
    column_list = get_row_value(metadata, "column_list")

    business_keys = split_config_list(business_key)
    configured_columns = split_config_list(column_list)

    if not business_keys:
        raise RuntimeError("[ERROR] business_key trong config_gold_tables đang trống")

    if not configured_columns:
        raise RuntimeError("[ERROR] column_list trong config_gold_tables đang trống")

    silver_table = f"{METADATA_DB}.{source_schema}.{source_object}"
    gold_table = f"{METADATA_DB}.{target_schema}.{target_object}"

    print(f"[CONFIG] Source table       : {silver_table}")
    print(f"[CONFIG] Target table       : {gold_table}")
    print(f"[CONFIG] Business key       : {business_keys}")

except Exception as e:
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
    raise

# ============================================================
# 5. Read changed records from Silver
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
        df_changed_silver = df_silver.filter(F.col("audit_ts") > F.lit(gold_watermark_ts))

    if df_changed_silver.isEmpty():
        print(f"[INFO] Không có dữ liệu Silver mới cho {TARGET_OBJECT}")
        raise SystemExit("[STOP] No new data")

    max_source_audit_ts = df_changed_silver.agg(F.max("audit_ts")).collect()[0][0]
    print(f"[INFO] Max source audit_ts from Silver: {max_source_audit_ts}")

except SystemExit:
    raise

except Exception as e:
    raise

# ============================================================
# 6. Prepare staged data
# ============================================================

try:
    source_cols = df_changed_silver.columns

    missing_bk = [c for c in business_keys if c not in source_cols]
    if missing_bk:
        raise RuntimeError(f"[ERROR] Missing business key columns in Silver: {missing_bk}")

    existing_configured_cols = [c for c in configured_columns if c in source_cols]

    technical_exclude_for_hash = {
        SKEY_COL,
        "audit_ts",
        "updated_audit_ts",
        "deleted_audit_ts",
        "scd_start",
        "scd_from",
        "scd_to",
        "scd_active",
        "scd_version",
        "inferred_flag",
        "hash_key",
        "row_hash",
    }

    hash_columns = [
        c for c in existing_configured_cols
        if c not in technical_exclude_for_hash
    ]

    if not hash_columns:
        raise RuntimeError("[ERROR] Không có business attribute nào để tạo row_hash")

    selected_cols = []
    for c in business_keys + existing_configured_cols + ["audit_ts", "deleted_audit_ts"]:
        if c in source_cols and c not in selected_cols:
            selected_cols.append(c)

    df_staged_raw = df_changed_silver.select(*selected_cols)

    df_staged_raw = df_staged_raw.withColumn(
        "hash_key",
        create_hash_expr(business_keys)
    )

    df_staged_raw = df_staged_raw.withColumn(
        "row_hash",
        create_hash_expr(hash_columns)
    )

    if "deleted_audit_ts" not in df_staged_raw.columns:
        df_staged_raw = df_staged_raw.withColumn("deleted_audit_ts", F.lit(None).cast(TimestampType()))
    else:
        df_staged_raw = df_staged_raw.withColumn("deleted_audit_ts", F.to_timestamp(F.col("deleted_audit_ts")))

    # Giữ latest record trong batch hiện tại theo business key, đúng tinh thần template SCD2
    w_latest = Window.partitionBy("hash_key").orderBy(
        F.col("audit_ts").desc()
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

except Exception as e:
    raise

# ============================================================
# 7. Create Gold schema if needed
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.gold")
    print("[INFO] Gold schema checked")

except Exception as e:
    raise

# ============================================================
# 8. Fill inferred Dim record
# ============================================================

try:
    inferred_filled_rows = 0

    if table_exists(gold_table):
        df_gold = spark.table(gold_table)
        if "inferred_flag" in df_gold.columns:
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

                for c in existing_configured_cols:
                    if c in df_inferred_to_fill.columns and c in df_gold.columns:
                        update_set[c] = f"s.`{c}`"

                DeltaTable.forName(spark, gold_table).alias("t") \
                    .merge(
                        df_inferred_to_fill.alias("s"),
                        f"t.{SKEY_COL} = s.{SKEY_COL}"
                    ) \
                    .whenMatchedUpdate(set=update_set) \
                    .execute()

                df_inferred_keys = df_inferred_to_fill.select("hash_key").distinct()
                df_staged_after_inferred = df_staged.join(df_inferred_keys, on="hash_key", how="left_anti")

            else:
                df_staged_after_inferred = df_staged

            df_inferred_to_fill.unpersist()

        else:
            print("[INFERRED] Target table không có inferred_flag, skip step")
            df_staged_after_inferred = df_staged

    else:
        print("[INFERRED] Target table chưa tồn tại, skip step")
        df_staged_after_inferred = df_staged

except Exception as e:
    raise

# ============================================================
# 9. Check new / changed / deleted records
# ============================================================

try:
    df_non_deleted_staged = df_staged_after_inferred.filter(F.col("deleted_audit_ts").isNull()).cache()
    df_deleted_staged = df_staged_after_inferred.filter(F.col("deleted_audit_ts").isNotNull()).cache()

    if table_exists(gold_table):
        df_gold_all = spark.table(gold_table)

        df_gold_keys = df_gold_all.select("hash_key").distinct()

        df_gold_active = (
            df_gold_all
            .filter(
                (F.col("scd_active") == 1) &
                (F.col("deleted_audit_ts").isNull())
            )
            .select(
                "hash_key",
                SKEY_COL,
                F.col("row_hash").alias("_gold_row_hash")
            )
        )

        df_new = (
            df_non_deleted_staged
            .join(df_gold_keys, on="hash_key", how="left_anti")
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
            .join(df_gold_active.select("hash_key", SKEY_COL), on="hash_key", how="inner")
            .cache()
        )

    else:
        df_new = df_non_deleted_staged.cache()
        df_changed = spark.createDataFrame([], df_non_deleted_staged.schema)
        df_deleted = spark.createDataFrame([], df_deleted_staged.schema)

    new_rows = df_new.count()
    changed_rows = df_changed.count()
    deleted_rows = df_deleted.count()

    print(f"[CHECK] New records     : {new_rows}")
    print(f"[CHECK] Changed records : {changed_rows}")
    print(f"[CHECK] Deleted records : {deleted_rows}")
    print(f"[CHECK] Inferred filled : {inferred_filled_rows}")

except Exception as e:
    raise

# ============================================================
# 10. Insert new records
# ============================================================

try:
    def build_scd2_insert_df(df_input):
        df_output = (
            df_input
            .withColumn("updated_audit_ts", F.lit(None).cast(TimestampType()))
            .withColumn("scd_start", F.current_timestamp())
            .withColumn("scd_from", F.to_timestamp(F.col("_effective_ts")))
            .withColumn("scd_to", F.to_timestamp(F.lit(DEFAULT_SCD_TO)))
            .withColumn("scd_active", F.lit(1).cast(IntegerType()))
            .withColumn("scd_version", F.lit(1).cast(IntegerType()))
            .withColumn("inferred_flag", F.lit(0).cast(IntegerType()))
            .drop("_effective_ts")
        )

        return df_output

    df_new_insert = build_scd2_insert_df(df_new)

    print(f"[INSERT NEW] Prepared rows: {new_rows}")

except Exception as e:
    raise

# ============================================================
# 11. Insert changed records as SCD2 versions
# ============================================================

try:
    df_changed_insert = build_scd2_insert_df(df_changed)

    df_to_insert = df_new_insert.unionByName(df_changed_insert, allowMissingColumns=True)

    insert_rows = df_to_insert.count()

    if insert_rows > 0:
        df_to_insert = add_surrogate_key(df_to_insert, gold_table, SKEY_COL)

        # Đưa surrogate key lên đầu
        ordered_cols = [SKEY_COL] + [c for c in df_to_insert.columns if c != SKEY_COL]
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
    raise

# ============================================================
# 12. Handle deleted records from Silver tombstone
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
    raise

# ============================================================
# 13. Recalculate SCD2 columns
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

        w_part = Window.partitionBy("hash_key")
        w_order = Window.partitionBy("hash_key").orderBy(
            F.col("scd_from").asc(),
            F.col("audit_ts").asc(),
            F.col(SKEY_COL).asc()
        )

        df_recalc = (
            df_target_affected
            .withColumn("_next_scd_from", F.lead("scd_from").over(w_order))
            .withColumn("_new_scd_version", F.row_number().over(w_order))
            .withColumn("_min_scd_start", F.min("scd_start").over(w_part))
            .withColumn(
                "_new_scd_to",
                F.when(F.col("deleted_audit_ts").isNotNull(), F.col("deleted_audit_ts"))
                 .when(F.col("_next_scd_from").isNotNull(), F.col("_next_scd_from"))
                 .otherwise(F.to_timestamp(F.lit(DEFAULT_SCD_TO)))
            )
            .withColumn(
                "_new_scd_active",
                F.when(F.col("deleted_audit_ts").isNotNull(), F.lit(0))
                 .when(F.col("_next_scd_from").isNull(), F.lit(1))
                 .otherwise(F.lit(0))
            )
            .select(
                SKEY_COL,
                F.col("_min_scd_start").alias("new_scd_start"),
                F.col("scd_from").alias("new_scd_from"),
                F.col("_new_scd_to").alias("new_scd_to"),
                F.col("_new_scd_active").cast(IntegerType()).alias("new_scd_active"),
                F.col("_new_scd_version").cast(IntegerType()).alias("new_scd_version")
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
    raise

# ============================================================
# 14. Update watermark
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
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
