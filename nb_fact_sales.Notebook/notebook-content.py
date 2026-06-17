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
# GOLD NOTEBOOK — FACT_SALES
# Source: Silver.Sales_Orders + Silver.Sales_OrderLines
# Load pattern:
#   - Orders and OrderLines are incremental Silver tables
#   - No soft delete handling for Orders / OrderLines
#   - Rebuild affected fact rows by delete + insert
#   - Lookup dimension surrogate keys from Gold dimensions
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, DecimalType
from delta.tables import DeltaTable
from datetime import datetime

# ============================================================
# 1. Hardcode parameters
# ============================================================

METADATA_DB = "lh_vule_sonle_medallion"

SOURCE_OBJECT_ORDERS = "Sales_Orders"
SOURCE_OBJECT_ORDERLINES = "Sales_OrderLines"
TARGET_OBJECT = "Fact_Sales"
TARGET_SCHEMA = "Gold"

CONFIG_TABLE = f"{METADATA_DB}.etl.config_gold_tables"
WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"

gold_table = f"{METADATA_DB}.{TARGET_SCHEMA}.{TARGET_OBJECT}"
silver_orders_table = f"{METADATA_DB}.Silver.{SOURCE_OBJECT_ORDERS}"
silver_orderlines_table = f"{METADATA_DB}.Silver.{SOURCE_OBJECT_ORDERLINES}"

# Fact grain: 1 row per sales order line
FACT_SKEY_COL = "fact_sales_skey"
FACT_BUSINESS_KEYS = ["OrderLineID"]

# Dimension table names. If a dimension does not exist yet, skey will be -1.
DIM_CUSTOMER_TABLE = f"{METADATA_DB}.Gold.Dim_Customer"
DIM_PRODUCT_TABLE = f"{METADATA_DB}.Gold.Dim_StockItems"   # đổi thành Dim_Product nếu project của bạn dùng tên đó
DIM_PERSON_TABLE = f"{METADATA_DB}.Gold.Dim_People"

DEFAULT_SCD_TO = "2999-12-31 00:00:00"

execution_start_time = datetime.now()
no_new_data = False

print("=" * 80)
print("[START] Gold fact load for FACT_SALES")
print(f"[SOURCE_OBJECT_ORDERS]     {SOURCE_OBJECT_ORDERS}")
print(f"[SOURCE_OBJECT_ORDERLINES] {SOURCE_OBJECT_ORDERLINES}")
print(f"[TARGET_OBJECT]            {TARGET_OBJECT}")
print(f"[START_TIME]               {execution_start_time}")
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


def source_change_ts_expr():
    # Orders / OrderLines ở Silver là incremental load và không handle soft delete,
    # nên chỉ dùng audit_ts làm change timestamp.
    return F.to_timestamp(F.col("audit_ts"))


def get_max_skey(target_table: str, skey_col: str) -> int:
    if not table_exists(target_table):
        return 0

    if skey_col not in spark.table(target_table).columns:
        return 0

    result = (
        spark.table(target_table)
        .agg(F.max(F.col(skey_col)).alias("max_skey"))
        .collect()[0]["max_skey"]
    )

    return int(result or 0)


def add_surrogate_key(df, target_table: str, skey_col: str, order_cols: list):
    max_skey = get_max_skey(target_table, skey_col)

    sort_cols = []
    for c in order_cols:
        if c in df.columns:
            sort_cols.append(F.col(c).asc())

    sort_cols += [F.col("hash_key").asc()]
    w_skey = Window.orderBy(*sort_cols)

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


def latest_snapshot(df, partition_cols: list, audit_col: str = "audit_ts"):
    missing = [c for c in partition_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"[ERROR] Missing partition columns for latest snapshot: {missing}")

    if audit_col not in df.columns:
        raise RuntimeError(f"[ERROR] Missing audit column: {audit_col}")

    w = Window.partitionBy(*partition_cols).orderBy(
        F.to_timestamp(F.col(audit_col)).desc()
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def lookup_scd2_dimension(
    df,
    dim_table: str,
    source_key_col: str,
    dim_key_col: str,
    skey_col: str,
    effective_ts_col: str
):
    # Nếu dimension chưa có, vẫn cho fact chạy và gán -1.
    if not table_exists(dim_table):
        print(f"[DIM LOOKUP] Dimension not found, default {skey_col} = -1: {dim_table}")
        return df.withColumn(skey_col, F.lit(-1).cast(IntegerType()))

    dim_df = spark.table(dim_table)

    required_cols = {dim_key_col, skey_col}
    missing = [c for c in required_cols if c not in dim_df.columns]

    if missing:
        print(f"[DIM LOOKUP] Dimension missing columns {missing}, default {skey_col} = -1: {dim_table}")
        return df.withColumn(skey_col, F.lit(-1).cast(IntegerType()))

    if "scd_from" in dim_df.columns and "scd_to" in dim_df.columns:
        dim_current = dim_df.select(
            F.col(dim_key_col).alias(f"dim_{dim_key_col}"),
            F.col(skey_col).alias(f"dim_{skey_col}"),
            F.to_timestamp(F.col("scd_from")).alias("dim_scd_from"),
            F.to_timestamp(F.col("scd_to")).alias("dim_scd_to")
        )

        joined = (
            df
            .join(
                F.broadcast(dim_current),
                (df[source_key_col] == F.col(f"dim_{dim_key_col}")) &
                (F.col(effective_ts_col) > F.col("dim_scd_from")) &
                (F.col(effective_ts_col) <= F.col("dim_scd_to")),
                how="left"
            )
            .withColumn(
                skey_col,
                F.coalesce(F.col(f"dim_{skey_col}"), F.lit(-1)).cast(IntegerType())
            )
            .drop(
                f"dim_{dim_key_col}",
                f"dim_{skey_col}",
                "dim_scd_from",
                "dim_scd_to"
            )
        )

    else:
        dim_current = (
            dim_df
            .select(
                F.col(dim_key_col).alias(f"dim_{dim_key_col}"),
                F.col(skey_col).alias(f"dim_{skey_col}")
            )
            .dropDuplicates([f"dim_{dim_key_col}"])
        )

        joined = (
            df
            .join(
                F.broadcast(dim_current),
                df[source_key_col] == F.col(f"dim_{dim_key_col}"),
                how="left"
            )
            .withColumn(
                skey_col,
                F.coalesce(F.col(f"dim_{skey_col}"), F.lit(-1)).cast(IntegerType())
            )
            .drop(f"dim_{dim_key_col}", f"dim_{skey_col}")
        )

    print(f"[DIM LOOKUP] Completed lookup: {dim_table} -> {skey_col}")
    return joined

# ============================================================
# 3. Load Gold config for Orders and OrderLines
#    Config vẫn được đọc để giữ cùng pattern với nb_dim_customer.
#    Fact grain sẽ lấy OrderLineID làm business key chính.
# ============================================================

try:
    config_rows = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("source_object").isin(SOURCE_OBJECT_ORDERS, SOURCE_OBJECT_ORDERLINES)) &
            (F.col("is_active") == 1)
        )
        .collect()
    )

    config_map = {
        get_row_value(r, "source_object"): r
        for r in config_rows
    }

    orders_config = config_map.get(SOURCE_OBJECT_ORDERS)
    orderlines_config = config_map.get(SOURCE_OBJECT_ORDERLINES)

    if orders_config is None:
        raise RuntimeError(f"[ERROR] Không tìm thấy config cho {SOURCE_OBJECT_ORDERS}")

    if orderlines_config is None:
        raise RuntimeError(f"[ERROR] Không tìm thấy config cho {SOURCE_OBJECT_ORDERLINES}")

    orders_business_keys = split_config_list(
        get_row_value(orders_config, "business_key")
    )

    orderlines_business_keys = split_config_list(
        get_row_value(orderlines_config, "business_key")
    )

    orders_configured_columns = split_config_list(
        get_row_value(orders_config, "column_list")
    )

    orderlines_configured_columns = split_config_list(
        get_row_value(orderlines_config, "column_list")
    )

    if "OrderLineID" in orderlines_business_keys:
        FACT_BUSINESS_KEYS = ["OrderLineID"]
    elif orderlines_business_keys:
        FACT_BUSINESS_KEYS = orderlines_business_keys

    print(f"[CONFIG] Orders table           : {silver_orders_table}")
    print(f"[CONFIG] OrderLines table       : {silver_orderlines_table}")
    print(f"[CONFIG] Orders business key    : {orders_business_keys}")
    print(f"[CONFIG] OrderLines business key: {orderlines_business_keys}")
    print(f"[CONFIG] Fact business key      : {FACT_BUSINESS_KEYS}")
    print(f"[CONFIG] Target table           : {gold_table}")

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
# 5. Read records from Silver Orders and OrderLines
# ============================================================

try:
    if not table_exists(silver_orders_table):
        raise RuntimeError(f"[ERROR] Source Silver table không tồn tại: {silver_orders_table}")

    if not table_exists(silver_orderlines_table):
        raise RuntimeError(f"[ERROR] Source Silver table không tồn tại: {silver_orderlines_table}")

    df_orders = spark.table(silver_orders_table)
    df_orderlines = spark.table(silver_orderlines_table)

    for table_name, df in [
        (silver_orders_table, df_orders),
        (silver_orderlines_table, df_orderlines)
    ]:
        if "audit_ts" not in df.columns:
            raise RuntimeError(f"[ERROR] Silver table không có audit_ts: {table_name}")

        if "OrderID" not in df.columns:
            raise RuntimeError(f"[ERROR] Silver table không có OrderID: {table_name}")

    if gold_watermark_ts is None:
        df_changed_orders = df_orders
        df_changed_orderlines = df_orderlines
    else:
        df_changed_orders = df_orders.filter(
            source_change_ts_expr() > F.lit(gold_watermark_ts)
        )

        df_changed_orderlines = df_orderlines.filter(
            source_change_ts_expr() > F.lit(gold_watermark_ts)
        )

    changed_orders_count = df_changed_orders.count()
    changed_orderlines_count = df_changed_orderlines.count()

    if changed_orders_count == 0 and changed_orderlines_count == 0:
        no_new_data = True

        execution_end_time = datetime.now()

        print("=" * 80)
        print("[SUCCESS] Gold fact load completed - No new data")
        print("[STOP] No new data")
        print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
        print(f"[START_TIME] {execution_start_time}")
        print(f"[END_TIME] {execution_end_time}")
        print(f"[DURATION] {execution_end_time - execution_start_time}")
        print("=" * 80)

    else:
        max_orders_audit_ts = (
            df_changed_orders
            .agg(F.max(F.to_timestamp(F.col("audit_ts"))).alias("max_ts"))
            .collect()[0]["max_ts"]
        ) if changed_orders_count > 0 else None

        max_orderlines_audit_ts = (
            df_changed_orderlines
            .agg(F.max(F.to_timestamp(F.col("audit_ts"))).alias("max_ts"))
            .collect()[0]["max_ts"]
        ) if changed_orderlines_count > 0 else None

        max_candidates = [
            x for x in [max_orders_audit_ts, max_orderlines_audit_ts]
            if x is not None
        ]

        max_source_audit_ts = max(max_candidates)

        print(f"[INFO] Changed Orders rows     : {changed_orders_count}")
        print(f"[INFO] Changed OrderLines rows : {changed_orderlines_count}")
        print(f"[INFO] Max source audit_ts     : {max_source_audit_ts}")

except Exception as e:
    print(f"[FAILED] Read changed Silver records failed: {str(e)}")
    raise


# ============================================================
# Stop notebook successfully if no new data
# ============================================================

if no_new_data:
    notebookutils.notebook.exit("NO_NEW_DATA")

# ============================================================
# 7. Join changed Orders + changed OrderLines and prepare fact base
# ============================================================

try:
    o = df_changed_orders.alias("o")
    ol = df_changed_orderlines.alias("ol")

    df_joined = (
        ol
        .join(o, on="OrderID", how="inner")
        .selectExpr(
            "ol.*",
            "o.CustomerID as CustomerID",
            "o.OrderDate as OrderDate",
            "o.ExpectedDeliveryDate as ExpectedDeliveryDate",
            "o.SalespersonPersonID as SalespersonPersonID",
            "o.audit_ts as orders_audit_ts",
            "o.source_id as orders_source_id"
        )
    )

    # audit_ts của fact lấy max giữa audit_ts của orderline và order.
    line_audit_col = F.to_timestamp(F.col("audit_ts"))
    order_audit_col = F.to_timestamp(F.col("orders_audit_ts"))

    df_fact_base = (
        df_joined
        .withColumn("audit_ts", F.greatest(line_audit_col, order_audit_col))
        .withColumn("updated_audit_ts", F.current_timestamp())
        .withColumn(
            "source_id",
            F.coalesce(
                F.col("source_id"),
                F.col("orders_source_id"),
                F.lit(-1)
            ).cast(IntegerType())
        )
        .withColumn("OrderDate", F.to_date(F.col("OrderDate")))
        .withColumn("ExpectedDeliveryDate", F.to_date(F.col("ExpectedDeliveryDate")))
        .withColumn(
            "fact_effective_ts",
            F.coalesce(
                F.to_timestamp(F.col("OrderDate")),
                F.to_timestamp(F.col("audit_ts"))
            )
        )
    )

    # Measures.
    df_fact_base = (
        df_fact_base
        .withColumn(
            "Quantity",
            F.coalesce(F.col("Quantity"), F.lit(0)).cast(DecimalType(18, 4))
        )
        .withColumn(
            "UnitPrice",
            F.coalesce(F.col("UnitPrice"), F.lit(0)).cast(DecimalType(18, 4))
        )
        .withColumn(
            "TaxRate",
            F.coalesce(F.col("TaxRate"), F.lit(0)).cast(DecimalType(18, 4))
        )
        .withColumn(
            "PickedQuantity",
            F.coalesce(F.col("PickedQuantity"), F.lit(0)).cast(DecimalType(18, 4))
        )
        .withColumn(
            "sales_amount_net",
            (F.col("Quantity") * F.col("UnitPrice")).cast(DecimalType(18, 4))
        )
        .withColumn(
            "tax_amount",
            (
                F.col("sales_amount_net") *
                F.col("TaxRate") /
                F.lit(100)
            ).cast(DecimalType(18, 4))
        )
        .withColumn(
            "sales_amount_gross",
            (
                F.col("sales_amount_net") +
                F.col("tax_amount")
            ).cast(DecimalType(18, 4))
        )
    )

    fact_base_count = df_fact_base.count()

    print(f"[FACT BASE] Joined changed Orders and changed OrderLines: {fact_base_count}")

except Exception as e:
    print(f"[FAILED] Join changed Orders + changed OrderLines failed: {str(e)}")
    raise


# ============================================================
# 9. Lookup surrogate keys from Gold dimensions
# ============================================================

try:
    # Customer dimension from Dim_Customer.
    df_fact_dim = lookup_scd2_dimension(
        df=df_fact_base,
        dim_table=DIM_CUSTOMER_TABLE,
        source_key_col="CustomerID",
        dim_key_col="CustomerID",
        skey_col="customer_skey",
        effective_ts_col="fact_effective_ts"
    )

    # Product / StockItem dimension.
    df_fact_dim = lookup_scd2_dimension(
        df=df_fact_dim,
        dim_table=DIM_PRODUCT_TABLE,
        source_key_col="StockItemID",
        dim_key_col="StockItemID",
        skey_col="stockitem_skey",
        effective_ts_col="fact_effective_ts"
    )

    # Salesperson dimension.
    df_fact_dim = lookup_scd2_dimension(
        df=df_fact_dim,
        dim_table=DIM_PERSON_TABLE,
        source_key_col="SalespersonPersonID",
        dim_key_col="PersonID",
        skey_col="person_skey",
        effective_ts_col="fact_effective_ts"
    )

    print("[DIM LOOKUP] Completed all dimension lookups")

except Exception as e:
    print(f"[FAILED] Lookup dimension surrogate keys failed: {str(e)}")
    raise

# ============================================================
# 10. Prepare staged fact rows
# ============================================================

try:
    # Fact hash key follows the fact grain.
    missing_fact_keys = [
        c for c in FACT_BUSINESS_KEYS
        if c not in df_fact_dim.columns
    ]

    if missing_fact_keys:
        raise RuntimeError(f"[ERROR] Missing fact business keys: {missing_fact_keys}")

    row_hash_cols = [
        c for c in [
            "OrderID",
            "OrderLineID",
            "CustomerID",
            "StockItemID",
            "SalespersonPersonID",
            "OrderDate",
            "ExpectedDeliveryDate",
            "Quantity",
            "UnitPrice",
            "TaxRate",
            "PickedQuantity",
            "customer_skey",
            "stockitem_skey",
            "person_skey",
            "sales_amount_net",
            "tax_amount",
            "sales_amount_gross"
        ]
        if c in df_fact_dim.columns
    ]

    df_staged = (
        df_fact_dim
        .withColumn("hash_key", create_hash_expr(FACT_BUSINESS_KEYS))
        .withColumn("row_hash", create_hash_expr(row_hash_cols))
        .select(
            "hash_key",
            "row_hash",
            "audit_ts",
            "updated_audit_ts",
            "source_id",
            "OrderID",
            *(["OrderLineID"] if "OrderLineID" in df_fact_dim.columns else []),
            *(["CustomerID"] if "CustomerID" in df_fact_dim.columns else []),
            *(["StockItemID"] if "StockItemID" in df_fact_dim.columns else []),
            *(["SalespersonPersonID"] if "SalespersonPersonID" in df_fact_dim.columns else []),
            "customer_skey",
            "stockitem_skey",
            "person_skey",
            *(["OrderDate"] if "OrderDate" in df_fact_dim.columns else []),
            *(["ExpectedDeliveryDate"] if "ExpectedDeliveryDate" in df_fact_dim.columns else []),
            *(["Description"] if "Description" in df_fact_dim.columns else []),
            "Quantity",
            "UnitPrice",
            "TaxRate",
            "PickedQuantity",
            "sales_amount_net",
            "tax_amount",
            "sales_amount_gross",
            "fact_effective_ts"
        )
        .dropDuplicates(["hash_key"])
        .cache()
    )

    staged_count = df_staged.count()

    print(f"[STAGED] Prepared fact rows: {staged_count}")
    print(f"[STAGED] Row hash columns: {row_hash_cols}")

except Exception as e:
    print(f"[FAILED] Prepare staged fact rows failed: {str(e)}")
    raise


# ============================================================
# 11. Create Gold schema if needed
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.{TARGET_SCHEMA}")
    print("[INFO] Gold schema checked")

except Exception as e:
    print(f"[FAILED] Create Gold schema failed: {str(e)}")
    raise


# ============================================================
# 12. Delete existing fact rows by fact hash_key
# ============================================================

try:
    deleted_rows = 0

    affected_fact_keys_df = (
        df_staged
        .select("hash_key")
        .dropDuplicates(["hash_key"])
        .cache()
    )

    affected_fact_key_count = affected_fact_keys_df.count()

    if table_exists(gold_table) and affected_fact_key_count > 0:
        df_existing_to_delete = (
            spark.table(gold_table)
            .join(F.broadcast(affected_fact_keys_df), on="hash_key", how="inner")
        )

        deleted_rows = df_existing_to_delete.count()

        DeltaTable.forName(spark, gold_table).alias("t") \
            .merge(
                affected_fact_keys_df.alias("s"),
                "t.hash_key = s.hash_key"
            ) \
            .whenMatchedDelete() \
            .execute()

        print(f"[DELETE] Deleted existing fact rows: {deleted_rows}")

    else:
        print("[DELETE] Target table not found or no affected keys; skip delete")

except Exception as e:
    print(f"[FAILED] Delete existing fact rows failed: {str(e)}")
    raise


# ============================================================
# 13. Insert refreshed fact rows
# ============================================================

try:
    insert_rows = staged_count

    if insert_rows > 0:
        df_to_insert = add_surrogate_key(
            df_staged,
            gold_table,
            FACT_SKEY_COL,
            FACT_BUSINESS_KEYS
        )

        ordered_cols = [FACT_SKEY_COL] + [
            c for c in df_to_insert.columns
            if c != FACT_SKEY_COL
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

        print(f"[INSERT] Inserted fact rows: {insert_rows}")

    else:
        print("[INSERT] No fact rows to insert")

except Exception as e:
    print(f"[FAILED] Insert fact rows failed: {str(e)}")
    raise


# ============================================================
# 14. Update Gold watermark
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
# 15. Summary and cleanup
# ============================================================

try:
    execution_end_time = datetime.now()

    print("=" * 80)
    print("[SUCCESS] Gold fact load completed")
    print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
    print(f"[START_TIME] {execution_start_time}")
    print(f"[END_TIME] {execution_end_time}")
    print(f"[DURATION] {execution_end_time - execution_start_time}")
    print("-" * 80)
    print(f"[SUMMARY] Changed Orders rows       : {changed_orders_count}")
    print(f"[SUMMARY] Changed OrderLines rows   : {changed_orderlines_count}")
    print(f"[SUMMARY] Changed latest Orders rows     : {orders_changed_latest_count}")
    print(f"[SUMMARY] Changed latest OrderLines rows : {orderlines_changed_latest_count}")
    print(f"[SUMMARY] Deleted old fact rows     : {deleted_rows}")
    print(f"[SUMMARY] Inserted refreshed rows   : {insert_rows}")
    print(f"[SUMMARY] Watermark updated         : {max_source_audit_ts}")
    print("=" * 80)

finally:
    for df_name in [
    "df_orders_changed_latest",
    "df_orderlines_changed_latest",
    "df_staged",
    "affected_fact_keys_df"
    ]:
        if df_name in locals():
            try:
                locals()[df_name].unpersist()
            except:
                pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
