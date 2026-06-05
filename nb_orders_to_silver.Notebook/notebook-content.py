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
# DEDICATED SILVER NOTEBOOK — SALES_ORDERS (DELTA LOAD)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

# ── 1. Hardcode sẵn (Chuyển sang dnyamic params sau) ──────────────────────
TARGET_OBJECT = "Sales_Orders"
METADATA_DB   = "lh_vule_sonle_medallion"
CONFIG_TABLE  = f"{METADATA_DB}.etl.config_silver_tables"
silver_table  = f"{METADATA_DB}.silver.{TARGET_OBJECT}"

print(f"[INFO] Bắt đầu kích hoạt Pipeline chuyên biệt cho thực thể: {silver_table}")


#--------------------------------------------------------------------------------------------

# ── 2. Lấy tham số từ config table ─────────────
config_row = (
    spark.table(CONFIG_TABLE)
    .filter(
        (F.col("source_object") == TARGET_OBJECT) &
        (F.col("is_active") == 1)
    )
    .collect()
)

if not config_row:
    raise RuntimeError(f"[ERROR] Không tìm thấy dòng cấu hình cho '{TARGET_OBJECT}' trong bảng {CONFIG_TABLE}!")

# Trích xuất các tham số nghiệp vụ của riêng bảng Customers
metadata = config_row[0]
source_system = metadata["source_system"] # Lấy thêm hệ thống nguồn (WWI)
business_key  = metadata["business_key"]
column_list   = metadata["column_list"]
load_type     = metadata["load_type"]

business_keys = [k.strip() for k in business_key.split(",")]
business_columns = sorted([c.strip() for c in column_list.split(",")])

# Cập nhật đường dẫn Bronze chuẩn theo cấu trúc: Bronze/System/Object/year/month/day
bronze_path = f"Files/Bronze/{source_system}/{TARGET_OBJECT}"
print(f"[INFO] Đường dẫn Bronze hiện tại: {bronze_path}")

#--------------------------------------------------------------------------------------------

# ── 3. ĐỌC BRONZE THEO WATERMARK & CÔ LẬP SNAPSHOT MỚI NHẤT ─────────────────

WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"

# =========================================
# GET SILVER WATERMARK
# =========================================

watermark_row = (
    spark.table(WATERMARK_TABLE)
    .filter(
        (F.col("layer") == "silver") &
        (F.col("object_name") == TARGET_OBJECT)
    )
    .orderBy(F.col("timestamp").desc())
    .limit(1)
    .select(
        F.to_timestamp("key_1").alias("max_audit_ts")
    )
    .collect()
)

max_audit_ts = watermark_row[0]["max_audit_ts"] if watermark_row else None

print(f"[INFO] Silver watermark hiện tại của {TARGET_OBJECT}: {max_audit_ts}")

# =========================================
# READ BRONZE
# =========================================

if max_audit_ts is None:
    bronze_raw_df = (
        spark.read.format("parquet")
        .load(bronze_path)
    )
else:
    load_year = max_audit_ts.year
    load_month = max_audit_ts.month
    load_day = max_audit_ts.day

    bronze_raw_df = (
        spark.read.format("parquet")
        .load(bronze_path)
        .filter(
            (
                F.col("year") > load_year
            )
            |
            (
                (F.col("year") == load_year)
                &
                (F.col("month") > load_month)
            )
            |
            (
                (F.col("year") == load_year)
                &
                (F.col("month") == load_month)
                &
                (F.col("day") >= load_day)
            )
        )
        .filter(
            F.col("audit_ts") > F.lit(max_audit_ts)
        )
    )

if bronze_raw_df.isEmpty():
    raise RuntimeError(
        f"[INFO] Không có dữ liệu Bronze mới hơn watermark cho {TARGET_OBJECT}. Dừng tiến trình."
    )

# =========================================
# GET LATEST SNAPSHOT TS FROM BRONZE
# =========================================

latest_audit_ts = (
    bronze_raw_df
    .select(
        F.max("audit_ts").alias("latest_audit_ts")
    )
    .collect()[0]["latest_audit_ts"]
)

if latest_audit_ts is None:
    raise RuntimeError(
        f"[ERROR] Không tìm thấy audit_ts hợp lệ trong Bronze của {TARGET_OBJECT} tại {bronze_path}"
    )

print(f"[INFO] Latest Bronze audit_ts: {latest_audit_ts}")

# =========================================
# DROP PARTITION COLUMNS
# =========================================

df_bronze_delta = (
    bronze_raw_df
    .drop(
        "year",
        "month",
        "day"
    )
)

if df_bronze_delta.isEmpty():
    raise RuntimeError(
        f"[ERROR] Bronze delta của {TARGET_OBJECT} bị trống sau khi filter watermark."
    )


#--------------------------------------------------------------------------------------------


# ── 4. DEDUPLICATE & ĐỔI MÃ HASH ─────────────────────────────
w_dedup = Window.partitionBy(*business_keys).orderBy(F.col("audit_ts").desc() if "audit_ts" in df_bronze_delta.columns else F.lit(1))
df_staged = (
    df_bronze_delta
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

key_expr = [F.coalesce(F.col(k).cast("string"), F.lit("NULL")) for k in business_keys]
df_staged = df_staged.withColumn("hash_key", F.sha2(F.concat_ws("||", *key_expr), 256))

hash_expr = [F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in business_columns if c in df_staged.columns]
df_staged = df_staged.withColumn("row_hash", F.sha2(F.concat_ws("||", *hash_expr), 256))

df_staged = (
    df_staged
    .withColumn("audit_ts", F.current_timestamp())
).cache()



#--------------------------------------------------------------------------------------------

# ── 5. ĐỐI CHIẾU VỚI CƠ SỞ DỮ LIỆU ĐÍCH SILVER ───────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.silver")

if not spark.catalog.tableExists(silver_table):
    print(f"[FIRST-RUN] Bảng '{silver_table}' chưa tồn tại. Tiến hành khởi tạo...")
    df_to_insert = df_staged
else:
    print(f"Bảng '{silver_table}' đã tồn tại. Đối chiếu thay đổi...")
    df_staged_keys = df_staged.select("hash_key").distinct()
    
    w_rank = Window.partitionBy("hash_key").orderBy(F.col("audit_ts").desc())
    df_silver_active = (
        spark.table(silver_table)
        .join(F.broadcast(df_staged_keys), on="hash_key", how="inner")
        .withColumn("_rn", F.row_number().over(w_rank))
        .filter(F.col("_rn") == 1)
        .select("hash_key", "row_hash")
        .cache()
    )

    df_new = df_staged.join(df_silver_active.select("hash_key"), on="hash_key", how="left_anti")
    df_changed = (
        df_staged
        .join(df_silver_active.withColumnRenamed("row_hash", "_silver_rh"), on="hash_key", how="inner")
        .filter(F.col("row_hash") != F.col("_silver_rh"))
        .drop("_silver_rh")
    )
    df_to_insert = df_new.unionByName(df_changed)

#--------------------------------------------------------------------------------------------

# ── 6. THỰC THI GHI APPEND-ONLY VÀO DELTA LAKE ───────────────
rows_to_insert = df_to_insert.count() if df_to_insert else 0

if rows_to_insert > 0:
    (
        df_to_insert
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )
    print(f"[SUCCESS] Đã cập nhật {rows_to_insert} dòng vào {silver_table}.")
else:
    print("[INFO] Không có dữ liệu mới/thay đổi. Bỏ qua ghi.")

df_staged.unpersist()
if 'df_silver_active' in locals(): df_silver_active.unpersist()

# ── 7. APPEND WATERMARK MỚI CHO SILVER ───────────────────────

watermark_new_df = (
    spark.createDataFrame(
        [
            (
                "silver",
                TARGET_OBJECT,
                str(latest_audit_ts),
                "datetime"
            )
        ],
        ["layer", "object_name", "key_1", "key_1_desc"]
    )
    .withColumn("timestamp", F.current_timestamp())
    .select("timestamp", "layer", "object_name", "key_1", "key_1_desc")
)

(
    watermark_new_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(WATERMARK_TABLE)
)

print(f"[SUCCESS] Đã append watermark mới cho {TARGET_OBJECT}: {latest_audit_ts}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
