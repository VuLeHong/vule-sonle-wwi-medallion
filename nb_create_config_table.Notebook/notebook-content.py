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
# Setup: etl.config_bronze_tables
# Run ONCE to create and seed the Bronze config table.
# ============================================================

from pyspark.sql import Row

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_silver_tables"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.etl")

# Drop and recreate config table
spark.sql(f"DROP TABLE IF EXISTS {CONFIG_TABLE}")

rows = [
    # ========================================================
    # Sales fact-related tables
    # Incremental load
    # ========================================================
    Row(
        config_id=1,
        source_system="WWI",
        source_schema="Sales",
        source_table="Orders",
        source_object="Sales_Orders",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderID",
        column_list=(
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, "
            "DeliveryInstructions, InternalComments, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    Row(
        config_id=2,
        source_system="WWI",
        source_schema="Sales",
        source_table="OrderLines",
        source_object="Sales_OrderLines",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderLineID",
        column_list=(
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    # ========================================================
    # Customer dimension tables
    # Full load
    # ========================================================
    Row(
        config_id=3,
        source_system="WWI",
        source_schema="Sales",
        source_table="Customers",
        source_object="Sales_Customers",
        load_type="full",
        watermark_column=None,
        business_key="CustomerID",
        column_list=(
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, DeliveryRun, "
            "RunPosition, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    Row(
        config_id=4,
        source_system="WWI",
        source_schema="Sales",
        source_table="CustomerCategories",
        source_object="Sales_CustomerCategories",
        load_type="full",
        watermark_column=None,
        business_key="CustomerCategoryID",
        column_list=(
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # Product / item dimension table
    # Full load
    # ========================================================
    Row(
        config_id=5,
        source_system="WWI",
        source_schema="Warehouse",
        source_table="StockItems",
        source_object="Warehouse_StockItems",
        load_type="full",
        watermark_column=None,
        business_key="StockItemID",
        column_list=(
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, MarketingComments, InternalComments, Photo, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # People dimension table
    # Full load
    # ========================================================
    Row(
        config_id=6,
        source_system="WWI",
        source_schema="Application",
        source_table="People",
        source_object="Application_People",
        load_type="full",
        watermark_column=None,
        business_key="PersonID",
        column_list=(
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, HashedPassword, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, Photo, CustomFields, OtherLanguages, LastEditedBy, "
            "ValidFrom, ValidTo"
        ),
        is_active=True
    ),
]

df = spark.createDataFrame(rows)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(CONFIG_TABLE)

print(f"Created {CONFIG_TABLE} with {df.count()} rows.")
spark.table(CONFIG_TABLE).orderBy("config_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Setup: etl.config_bronze_tables
# Run ONCE to create and seed the Bronze config table.
# ============================================================

from pyspark.sql import Row

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_silver_tables"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.etl")

# Drop and recreate config table
spark.sql(f"DROP TABLE IF EXISTS {CONFIG_TABLE}")

rows = [
    # ========================================================
    # Sales fact-related tables
    # Incremental load
    # ========================================================
    Row(
        config_id=1,
        source_system="WWI",
        source_schema="Sales",
        source_table="Orders",
        source_object="Sales_Orders",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderID",
        column_list=(
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, "
            "DeliveryInstructions, InternalComments, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    Row(
        config_id=2,
        source_system="WWI",
        source_schema="Sales",
        source_table="OrderLines",
        source_object="Sales_OrderLines",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderLineID",
        column_list=(
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    # ========================================================
    # Customer dimension tables
    # Full load
    # ========================================================
    Row(
        config_id=3,
        source_system="WWI",
        source_schema="Sales",
        source_table="Customers",
        source_object="Sales_Customers",
        load_type="full",
        watermark_column=None,
        business_key="CustomerID",
        column_list=(
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, DeliveryRun, "
            "RunPosition, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    Row(
        config_id=4,
        source_system="WWI",
        source_schema="Sales",
        source_table="CustomerCategories",
        source_object="Sales_CustomerCategories",
        load_type="full",
        watermark_column=None,
        business_key="CustomerCategoryID",
        column_list=(
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # Product / item dimension table
    # Full load
    # ========================================================
    Row(
        config_id=5,
        source_system="WWI",
        source_schema="Warehouse",
        source_table="StockItems",
        source_object="Warehouse_StockItems",
        load_type="full",
        watermark_column=None,
        business_key="StockItemID",
        column_list=(
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, MarketingComments, InternalComments, Photo, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # People dimension table
    # Full load
    # ========================================================
    Row(
        config_id=6,
        source_system="WWI",
        source_schema="Application",
        source_table="People",
        source_object="Application_People",
        load_type="full",
        watermark_column=None,
        business_key="PersonID",
        column_list=(
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, HashedPassword, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, Photo, CustomFields, OtherLanguages, LastEditedBy, "
            "ValidFrom, ValidTo"
        ),
        is_active=True
    ),
]

df = spark.createDataFrame(rows)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(CONFIG_TABLE)

print(f"Created {CONFIG_TABLE} with {df.count()} rows.")
spark.table(CONFIG_TABLE).orderBy("config_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Setup: etl.config_gold_tables
# Run ONCE to create and seed the Bronze config table.
# ============================================================

from pyspark.sql import Row

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_gold_tables"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.etl")

# Drop and recreate config table
spark.sql(f"DROP TABLE IF EXISTS {CONFIG_TABLE}")

rows = [
    # ========================================================
    # Sales fact-related tables
    # Incremental load
    # ========================================================
    Row(
        config_id=1,
        source_system="WWI",
        source_object="Sales_Orders",
        watermark_column="LastEditedWhen",
        business_key="OrderID",
        column_list=(
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, "
            "PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    Row(
        config_id=2,
        source_system="WWI",
        source_object="Sales_OrderLines",
        watermark_column="LastEditedWhen",
        business_key="OrderLineID",
        column_list=(
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    # ========================================================
    # Customer dimension tables
    # Full load
    # ========================================================
    Row(
        config_id=3,
        source_system="WWI",
        source_object="Sales_Customers",
        watermark_column=None,
        business_key="CustomerID",
        column_list=(
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, "
            "DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    Row(
        config_id=4,
        source_system="WWI",
        source_object="Sales_CustomerCategories",
        watermark_column=None,
        business_key="CustomerCategoryID",
        column_list=(
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # Product / item dimension table
    # Full load
    # ========================================================
    Row(
        config_id=5,
        source_system="WWI",
        source_object="Warehouse_StockItems",
        watermark_column=None,
        business_key="StockItemID",
        column_list=(
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # People dimension table
    # Full load
    # ========================================================
    Row(
        config_id=6,
        source_system="WWI",
        source_object="Application_People",
        watermark_column=None,
        business_key="PersonID",
        column_list=(
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, LastEditedBy, "
            "ValidFrom, ValidTo"
        ),
        is_active=True
    ),
]

df = spark.createDataFrame(rows)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(CONFIG_TABLE)

print(f"Created {CONFIG_TABLE} with {df.count()} rows.")
spark.table(CONFIG_TABLE).orderBy("config_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

METADATA_DB = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_silver_tables"

# Định nghĩa column_list mới (đã bỏ DeliveryRun, RunPosition)
new_column_list = (
    "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
    "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
    "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
    "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
    "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, "
    "WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, "
    "DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, "
    "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
)

# Đọc config hiện tại
df_config = spark.table(CONFIG_TABLE)

# Cập nhật cột column_list cho config_id = 3
df_updated = df_config.withColumn(
    "column_list",
    F.when(F.col("config_id") == 3, F.lit(new_column_list)).otherwise(F.col("column_list"))
)

# Ghi đè bảng config
df_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(CONFIG_TABLE)

print("Đã cập nhật column_list cho Sales_Customers (removed DeliveryRun, RunPosition).")

# Kiểm tra kết quả
spark.table(CONFIG_TABLE).filter("config_id = 3").select("column_list").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
