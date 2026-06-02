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
CONFIG_TABLE = f"{METADATA_DB}.etl.config_bronze_tables"

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
# Deactivate old config row and append new config row
# ============================================================

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_bronze_tables"

# ============================================================
# 1. Deactivate old active row
# ============================================================

spark.sql(f"""
UPDATE {CONFIG_TABLE}
SET is_active = false
WHERE source_object = 'Sales_Customers'
  AND is_active = true
""")

# ============================================================
# 2. Append new row
# ============================================================

spark.sql(f"""
INSERT INTO {CONFIG_TABLE}
(
    config_id,
    source_system,
    source_schema,
    source_table,
    source_object,
    load_type,
    watermark_column,
    business_key,
    column_list,
    is_active
)
VALUES
(
    7,
    'WWI',
    'Sales',
    'Customers',
    'Sales_Customers',
    'full',
    NULL,
    'CustomerID',
    'CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, DeliveryRun, RunPosition, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, DeliveryPostalCode, PostalAddressLine1, PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo',
    true
)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
