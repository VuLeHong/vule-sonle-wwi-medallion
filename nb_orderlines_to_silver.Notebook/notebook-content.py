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
# DEDICATED SILVER NOTEBOOK — SALES_ORDERLINES (INCREMENTAL)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType, DateType

# ============================================================
# CLEANING
# ============================================================

EXCLUDE_COLS = {
}

def clean_strings(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", None)
                 .otherwise(F.trim(F.col(field.name)))
            )
    return df


def cast_dates(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, (TimestampType, DateType)):
            df = df.withColumn(field.name, F.to_timestamp(F.col(field.name)))
    return df


def drop_unwanted_cols(df):
    all_drop = {c for c in df.columns if c.lower() in EXCLUDE_COLS}
    all_drop |= {c for c in ["year", "month", "day"] if c in df.columns}

    if all_drop:
        df = df.drop(*all_drop)

    return df


# ============================================================
# 1. Hardcode
# ============================================================

TARGET_OBJECT = "Sales_OrderLines"
METADATA_DB   = "lh_vule_sonle_medallion"

CONFIG_TABLE = f"{METADATA_DB}.etl.config_silver_tables"
WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"

silver_schema = "Silver"
silver_table  = f"{METADATA_DB}.{silver_schema}.{TARGET_OBJECT}"

no_new_bronze_data = False

print("=" * 80)
print("[START] Silver incremental load started")
print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
print(f"[SILVER_TABLE] {silver_table}")
print("=" * 80)


# ============================================================
# 2. Lấy cấu hình từ metadata
# ============================================================

try:
    config_row = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("source_object") == TARGET_OBJECT) &
            (F.col("is_active") == 1)
        )
        .collect()
    )

    if not config_row:
        raise RuntimeError(f"[ERROR] Không tìm thấy config cho '{TARGET_OBJECT}'")

    metadata = config_row[0]

    source_system = metadata["source_system"]
    business_key  = metadata["business_key"]
    column_list   = metadata["column_list"]

    business_keys = [k.strip() for k in business_key.split(",") if k.strip()]
    business_columns = sorted([c.strip() for c in column_list.split(",") if c.strip()])

    bronze_path = f"Files/Bronze/{source_system}/{TARGET_OBJECT}"

    print(f"[CONFIG] Bronze path   : {bronze_path}")
    print(f"[CONFIG] Business keys : {business_keys}")

except Exception as e:
    print(f"[FAILED] Load Silver config failed: {str(e)}")
    raise


# ============================================================
# 3. Đọc Bronze theo watermark
# ============================================================

try:
    watermark_row = (
        spark.table(WATERMARK_TABLE)
        .filter(
            (F.col("layer") == "silver") &
            (F.col("object_name") == TARGET_OBJECT)
        )
        .orderBy(F.col("timestamp").desc())
        .limit(1)
        .select(F.to_timestamp("key_1").alias("max_audit_ts"))
        .collect()
    )

    max_audit_ts = watermark_row[0]["max_audit_ts"] if watermark_row else None

    print(f"[WATERMARK] Current Silver watermark: {max_audit_ts}")

    if max_audit_ts is None:
        bronze_raw_df = spark.read.parquet(bronze_path)
    else:
        load_year = max_audit_ts.year
        load_month = max_audit_ts.month
        load_day = max_audit_ts.day

        bronze_raw_df = (
            spark.read.parquet(bronze_path)
            .filter(
                (F.col("year") > load_year) |
                ((F.col("year") == load_year) & (F.col("month") > load_month)) |
                (
                    (F.col("year") == load_year) &
                    (F.col("month") == load_month) &
                    (F.col("day") >= load_day)
                )
            )
            .filter(F.col("audit_ts") > F.lit(max_audit_ts))
        )

    if bronze_raw_df.isEmpty():
        no_new_bronze_data = True

        print("=" * 80)
        print("[SUCCESS] Silver incremental load completed - No new Bronze data")
        print("[STOP] No new Bronze data")
        print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
        print(f"[SILVER_TABLE] {silver_table}")
        print(f"[CURRENT_WATERMARK] {max_audit_ts}")
        print("=" * 80)

    else:
        latest_audit_ts = (
            bronze_raw_df
            .agg(F.max("audit_ts").alias("latest_audit_ts"))
            .collect()[0]["latest_audit_ts"]
        )

        if latest_audit_ts is None:
            raise RuntimeError(f"[ERROR] Không tìm thấy audit_ts trong Bronze")

        print(f"[INFO] Latest audit_ts: {latest_audit_ts}")

except Exception as e:
    print(f"[FAILED] Read Bronze data failed: {str(e)}")
    raise


# ============================================================
# Stop notebook successfully if no new Bronze data
# IMPORTANT:
#   - Không dùng raise RuntimeError/SystemExit cho case no data
#   - Không đặt notebookutils.notebook.exit trong try/except
# ============================================================

if no_new_bronze_data:
    notebookutils.notebook.exit("NO_NEW_BRONZE_DATA")


# ============================================================
# 4. Prepare Bronze delta + cleaning
# ============================================================

try:
    df_bronze_delta = bronze_raw_df.drop("year", "month", "day")

    if df_bronze_delta.isEmpty():
        raise RuntimeError(f"[ERROR] Bronze delta trống sau filter.")

    df_bronze_delta = clean_strings(df_bronze_delta)
    df_bronze_delta = cast_dates(df_bronze_delta)
    df_bronze_delta = drop_unwanted_cols(df_bronze_delta)

    bronze_delta_count = df_bronze_delta.count()

    print(f"[INFO] Bronze delta rows: {bronze_delta_count}")

except Exception as e:
    print(f"[FAILED] Prepare Bronze delta failed: {str(e)}")
    raise


# ============================================================
# 5. Deduplicate & hash
# ============================================================

try:
    w_dedup = (
        Window
        .partitionBy(*business_keys)
        .orderBy(
            F.col("audit_ts").desc()
            if "audit_ts" in df_bronze_delta.columns
            else F.lit(1)
        )
    )

    df_staged = (
        df_bronze_delta
        .withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    key_expr = [
        F.coalesce(F.col(k).cast("string"), F.lit("NULL"))
        for k in business_keys
    ]

    df_staged = df_staged.withColumn(
        "hash_key",
        F.sha2(F.concat_ws("||", *key_expr), 256)
    )

    technical_cols = {
        "hash_key",
        "row_hash",
        "audit_ts",
        "year",
        "month",
        "day"
    }

    hash_columns = [
        c for c in business_columns
        if c in df_staged.columns and c.lower() not in technical_cols
    ]

    if not hash_columns:
        raise RuntimeError("[ERROR] Không có business columns để tạo row_hash")

    hash_expr = [
        F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
        for c in hash_columns
    ]

    df_staged = df_staged.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("||", *hash_expr), 256)
    )

    # audit_ts ở Silver là thời điểm load vào Silver
    df_staged = df_staged.withColumn("audit_ts", F.current_timestamp()).cache()

    staged_count = df_staged.count()

    print(f"[INFO] Số dòng sau deduplicate: {staged_count}")
    print(f"[INFO] Row hash columns: {hash_columns}")

except Exception as e:
    print(f"[FAILED] Deduplicate/hash failed: {str(e)}")
    raise


# ============================================================
# 6. Đối chiếu với Silver
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.{silver_schema}")

    if not spark.catalog.tableExists(silver_table):
        print(f"[FIRST-RUN] Tạo bảng {silver_table}")
        df_to_insert = df_staged

    else:
        print(f"[INFO] Bảng {silver_table} đã tồn tại. Đối chiếu thay đổi.")

        df_staged_keys = df_staged.select("hash_key").distinct()

        w_rank = (
            Window
            .partitionBy("hash_key")
            .orderBy(F.col("audit_ts").desc())
        )

        df_silver_active = (
            spark.table(silver_table)
            .join(F.broadcast(df_staged_keys), on="hash_key", how="inner")
            .withColumn("_rn", F.row_number().over(w_rank))
            .filter(F.col("_rn") == 1)
            .select("hash_key", "row_hash")
            .cache()
        )

        df_new = (
            df_staged
            .join(
                df_silver_active.select("hash_key"),
                on="hash_key",
                how="left_anti"
            )
        )

        df_changed = (
            df_staged
            .join(
                df_silver_active.withColumnRenamed("row_hash", "_silver_rh"),
                on="hash_key",
                how="inner"
            )
            .filter(F.col("row_hash") != F.col("_silver_rh"))
            .drop("_silver_rh")
        )

        df_to_insert = df_new.unionByName(df_changed)

except Exception as e:
    print(f"[FAILED] Compare with Silver failed: {str(e)}")
    raise


# ============================================================
# 7. Ghi vào Silver
# ============================================================

try:
    rows_to_insert = df_to_insert.count()

    if rows_to_insert > 0:
        (
            df_to_insert
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(silver_table)
        )

        print(f"[SUCCESS] Đã ghi {rows_to_insert} dòng vào {silver_table}")
    else:
        print("[INFO] Không có dòng mới hoặc dòng thay đổi. Bỏ qua ghi Silver.")

except Exception as e:
    print(f"[FAILED] Append to Silver failed: {str(e)}")
    raise


# ============================================================
# 8. Cập nhật watermark
# ============================================================

try:
    watermark_new_df = (
        spark.createDataFrame(
            [("silver", TARGET_OBJECT, str(latest_audit_ts), "datetime")],
            ["layer", "object_name", "key_1", "key_1_desc"]
        )
        .withColumn("timestamp", F.current_timestamp())
    )

    (
        watermark_new_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(WATERMARK_TABLE)
    )

    print(f"[SUCCESS] Watermark updated: {latest_audit_ts}")

except Exception as e:
    print(f"[FAILED] Update Silver watermark failed: {str(e)}")
    raise


# ============================================================
# 9. End log and cleanup
# ============================================================

try:
    print("=" * 80)
    print("[SUCCESS] Silver incremental load completed")
    print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
    print(f"[SILVER_TABLE] {silver_table}")
    print(f"[LATEST_BRONZE_AUDIT_TS] {latest_audit_ts}")
    print(f"[STAGED_ROWS] {staged_count}")
    print(f"[INSERTED_ROWS] {rows_to_insert}")
    print("=" * 80)

finally:
    if "df_staged" in locals():
        df_staged.unpersist()

    if "df_silver_active" in locals():
        df_silver_active.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
