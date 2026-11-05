# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Read data from Bronze tables
sales_df = spark.table("retail_catalog.bronze.raw_sales")

stores_df = spark.table("retail_catalog.bronze.dim_stores")

products_df = spark.table("retail_catalog.bronze.dim_products")

# COMMAND ----------

# Display schemas
print("SALES SCHEMA")
sales_df.printSchema()

print("STORES SCHEMA")
stores_df.printSchema()

print("PRODUCTS SCHEMA")
products_df.printSchema()

# COMMAND ----------

# Validate Bronze record counts

print("Sales count:", sales_df.count())

print("Stores count:", stores_df.count())

print("Products count:", products_df.count())

# COMMAND ----------

# Define window specification for sales deduplication

sales_window = Window.partitionBy("sale_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("_ingested_at").desc_nulls_last(),
    F.col("_source_file").desc_nulls_last()
)

# COMMAND ----------

# Assign row numbers to sales records

sales_ranked_df = sales_df.withColumn(
    "row_num",
    F.row_number().over(sales_window)
)

# COMMAND ----------

# Retain the latest record for each sale_id

sales_dedup_df = sales_ranked_df.filter(
    F.col("row_num") == 1
).drop("row_num")

# COMMAND ----------

# Validate deduplicated sales records

print("Original sales count:", sales_df.count())

print("Deduplicated sales count:", sales_dedup_df.count())

print(
    "Unique sale IDs:",
    sales_dedup_df.select("sale_id").distinct().count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Invalid Sales Records

# COMMAND ----------

# Find sales records with missing sale_id

missing_id_df = sales_dedup_df.filter(
    F.col("sale_id").isNull()
)

display(missing_id_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find missing amounts

# COMMAND ----------

# Find sales records with missing amount

missing_amount_df = sales_dedup_df.filter(
    F.col("amount").isNull()
)

display(missing_amount_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find zero or negative amounts

# COMMAND ----------

# Find sales records with zero or negative amounts

invalid_amount_df = sales_dedup_df.filter(
    F.col("amount") <= 0
)

display(invalid_amount_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create cleaned sales DataFrame

# COMMAND ----------

# Remove invalid sales records
quarantine_sales_df = sales_dedup_df.filter(
    (F.col("sale_id").isNull()) |
    (F.col("amount").isNull()) |
    (F.col("amount") <= 0)
)

# Keep only valid sales records
sales_clean_df = sales_dedup_df.filter(
    F.col("sale_id").isNotNull()
).filter(
    F.col("amount").isNotNull()
).filter(
    F.col("amount") > 0
)

print("Quarantined records:", quarantine_sales_df.count())
print("Clean sales records:", sales_clean_df.count())

# COMMAND ----------

# a rejection reason column

quarantine_sales_df = quarantine_sales_df.withColumn(
    "rejection_reason",
    F.when(F.col("sale_id").isNull(), "MISSING_SALE_ID")
     .when(F.col("amount").isNull(), "MISSING_AMOUNT")
     .when(F.col("amount") <= 0, "INVALID_AMOUNT")
)

# COMMAND ----------

quarantine_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_catalog.silver.quarantine_sales")

# COMMAND ----------

display(
    spark.table("retail_catalog.silver.quarantine_sales")
)

# COMMAND ----------

# Handle NULL payment modes
# Replace missing payment modes with UNKNOWN

sales_clean_df = sales_clean_df.fillna(
    "UNKNOWN",
    subset=["payment_mode"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC We are updating our temporary DataFrame, not modifying Bronze.

# COMMAND ----------

# Count missing payment modes

missing_payment_count = sales_clean_df.filter(
    F.col("payment_mode").isNull()
).count()

print("Remaining NULL payment modes:", missing_payment_count)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Store Dimension Cleansing**

# COMMAND ----------

# Find stores with missing store IDs

missing_store_df = stores_df.filter(
    F.col("store_id").isNull()
)

display(missing_store_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing store names

# COMMAND ----------

# Find stores with missing store names

missing_store_name_df = stores_df.filter(
    F.col("store_name").isNull()
)

display(missing_store_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing regions

# COMMAND ----------

# Find stores with missing regions

missing_region_df = stores_df.filter(
    F.col("region").isNull()
)

display(missing_region_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing cities

# COMMAND ----------

# Find stores with missing cities

missing_city_df = stores_df.filter(
    F.col("city").isNull()
)

display(missing_city_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing store types

# COMMAND ----------

# Find stores with missing store types

missing_store_type_df = stores_df.filter(
    F.col("store_type").isNull()
)

display(missing_store_type_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing is_active values

# COMMAND ----------

# Find stores with missing active status

missing_active_df = stores_df.filter(
    F.col("is_active").isNull()
)

display(missing_active_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check duplicate store IDs

# COMMAND ----------

# Count total and unique store IDs

total_stores = stores_df.count()

unique_stores = stores_df.select("store_id").distinct().count()

print("Total stores:", total_stores)
print("Unique store IDs:", unique_stores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create cleaned store DataFrame

# COMMAND ----------

# Create cleaned store DataFrame

stores_clean_df = stores_df.filter(
    F.col("store_id").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Product Dimension Cleansing**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing product IDs

# COMMAND ----------

# Find products with missing product IDs

missing_product_df = products_df.filter(
    F.col("product_id").isNull()
)

display(missing_product_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing product names

# COMMAND ----------

# Find products with missing product names

missing_product_name_df = products_df.filter(
    F.col("product_name").isNull()
)

display(missing_product_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing product categories

# COMMAND ----------

# Find products with missing categories

missing_category_df = products_df.filter(
    F.col("category").isNull()
)

display(missing_category_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing sub-categories

# COMMAND ----------

# Find products with missing sub-categories

missing_subcategory_df = products_df.filter(
    F.col("sub_category").isNull()
)

display(missing_subcategory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing brands

# COMMAND ----------

# Find products with missing brands

missing_brand_df = products_df.filter(
    F.col("brand").isNull()
)

display(missing_brand_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check missing MRP

# COMMAND ----------

# Find products with missing MRP

missing_mrp_df = products_df.filter(
    F.col("mrp").isNull()
)

display(missing_mrp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check invalid MRP values

# COMMAND ----------

# Find products with invalid MRP

invalid_mrp_df = products_df.filter(
    F.col("mrp") <= 0
)

display(invalid_mrp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check duplicate product IDs

# COMMAND ----------

# Check total and unique product IDs

total_products = products_df.count()

unique_products = products_df.select("product_id").distinct().count()

print("Total products:", total_products)
print("Unique product IDs:", unique_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create cleaned product DataFrame

# COMMAND ----------

# Create cleaned product DataFrame

products_clean_df = products_df.filter(
    F.col("product_id").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Join Sales with Stores**

# COMMAND ----------

# Give each source DataFrame an alias
s = sales_clean_df.alias("s")
st = stores_clean_df.alias("st")
p = products_clean_df.alias("p")

# Join the three datasets and select explicit columns
silver_sales_df = (
    s.join(st, on="store_id", how="left")
     .join(p, on="product_id", how="left")
     .select(
         # Sales information
         "sale_id",
         "sale_time",
         "store_id",
         "product_id",
         "quantity",
         "unit_price",
         "discount_pct",
         "amount",
         "payment_mode",
         "channel",
         "created_at",
         F.col("s.updated_at").alias("sales_updated_at"),

         # Store information
         "store_name",
         "city",
         "region",
         "store_type",
         "is_active",
         F.col("st.updated_at").alias("store_updated_at"),

         # Product information
         "product_name",
         "category",
         "sub_category",
         "brand",
         "mrp",
         F.col("p.updated_at").alias("product_updated_at"),

         # Sales ingestion metadata
         F.col("s._ingested_at").alias("sales_ingested_at"),
         F.col("s._source_file").alias("sales_source_file")
     )
)

# Inspect the resulting dataset
silver_sales_df.printSchema()

print("Silver record count:", silver_sales_df.count())

# COMMAND ----------

# 1. Check total records
print("Total Silver records:", silver_sales_df.count())

# 2. Check unique sales
print("Unique sale IDs:", silver_sales_df.select("sale_id").distinct().count())

# 3. Check missing store information
print(
    "Missing store names:",
    silver_sales_df.filter(F.col("store_name").isNull()).count()
)

# 4. Check missing product information
print(
    "Missing product names:",
    silver_sales_df.filter(F.col("product_name").isNull()).count()
)

# 5. Check missing revenue
print(
    "Missing amounts:",
    silver_sales_df.filter(F.col("amount").isNull()).count()
)

# 6. Check duplicate column names
print(
    "Total columns:",
    len(silver_sales_df.columns)
)

print(
    "Unique column names:",
    len(set(silver_sales_df.columns))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the Silver Delta table

# COMMAND ----------

# DBTITLE 1,Save Silver sales with MERGE
# Save silver_sales_df (built in the join cell above) into the Silver table.
# Existing sale_id -> update it, new sale_id -> insert it.
silver_sales_df.createOrReplaceTempView("silver_sales_source")

spark.sql("""
MERGE INTO retail_catalog.silver.sales AS target
USING silver_sales_source AS source
ON target.sale_id = source.sale_id

WHEN MATCHED THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# DBTITLE 1,Check Silver sales table
# MAGIC %sql
# MAGIC -- Check the Silver sales table
# MAGIC -- Good result: total_records = unique_sales (no duplicate sales)
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT sale_id) AS unique_sales
# MAGIC FROM retail_catalog.silver.sales;

# COMMAND ----------

# MAGIC %md
# MAGIC # **SCD Type 1**

# COMMAND ----------

# DBTITLE 1,Check store counts (history aware)
# MAGIC %sql
# MAGIC -- dim_stores keeps history (SCD Type 2), so one store can have many rows.
# MAGIC -- Only ONE row per store should be the current one (is_current = true).
# MAGIC -- Good result: unique_stores = current_rows
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT store_id) AS unique_stores,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_rows
# MAGIC FROM retail_catalog.silver.dim_stores;

# COMMAND ----------

# DBTITLE 1,SCD Type 1 explanation
# MAGIC %md
# MAGIC ## SCD Type 1 table (no history)
# MAGIC
# MAGIC SCD Type 1 = when a store changes, we simply **overwrite** the old values. No history is kept.
# MAGIC
# MAGIC We keep SCD Type 1 in its **own table** `retail_catalog.silver.dim_stores_scd1`, so it never mixes with the SCD Type 2 history table `retail_catalog.silver.dim_stores`.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Create the table the first time (does nothing if it already exists).
# MAGIC 2. MERGE the latest Bronze stores into it: update stores that exist, insert new stores.
# MAGIC 3. Check the result.

# COMMAND ----------

# DBTITLE 1,SCD1 Step 1 - create table if missing
# MAGIC %sql
# MAGIC -- Step 1: Create the SCD Type 1 table only if it does not exist yet.
# MAGIC -- If the table already exists, this does nothing.
# MAGIC CREATE TABLE IF NOT EXISTS retail_catalog.silver.dim_stores_scd1 AS
# MAGIC SELECT
# MAGIC     store_id,
# MAGIC     store_name,
# MAGIC     city,
# MAGIC     region,
# MAGIC     store_type,
# MAGIC     is_active,
# MAGIC     updated_at
# MAGIC FROM retail_catalog.bronze.dim_stores
# MAGIC WHERE store_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,SCD1 Step 2 - overwrite changed stores
# MAGIC %sql
# MAGIC -- Step 2: SCD Type 1 MERGE
# MAGIC -- Store already exists  -> overwrite it with the latest Bronze values
# MAGIC -- Store is new          -> insert it
# MAGIC MERGE INTO retail_catalog.silver.dim_stores_scd1 AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         store_id,
# MAGIC         store_name,
# MAGIC         city,
# MAGIC         region,
# MAGIC         store_type,
# MAGIC         is_active,
# MAGIC         updated_at
# MAGIC     FROM retail_catalog.bronze.dim_stores
# MAGIC     WHERE store_id IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.store_id = source.store_id
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# DBTITLE 1,SCD1 Step 3 - check result
# MAGIC %sql
# MAGIC -- Step 3: Check SCD Type 1 table
# MAGIC -- Good result: total_rows = unique_stores (one row per store, no history)
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT store_id) AS unique_stores
# MAGIC FROM retail_catalog.silver.dim_stores_scd1;

# COMMAND ----------

# MAGIC %md
# MAGIC # **SCD Type 2**

# COMMAND ----------

# DBTITLE 1,SCD Type 2 explanation
# MAGIC %md
# MAGIC ## SCD Type 2 table (keeps history)
# MAGIC
# MAGIC Table: `retail_catalog.silver.dim_stores`
# MAGIC
# MAGIC Every store row has 3 extra columns:
# MAGIC * `effective_start_date` = when this version started
# MAGIC * `effective_end_date` = when this version ended (`9999-12-31` means "still current")
# MAGIC * `is_current` = `true` only for the latest version
# MAGIC
# MAGIC When a store changes in Bronze:
# MAGIC 1. **Close** the old row (set end date, `is_current = false`).
# MAGIC 2. **Insert** a new row with the new values (`is_current = true`).
# MAGIC
# MAGIC Both steps use the **same time** (`load_time`), so the old row ends exactly when the new row starts (no gap).
# MAGIC
# MAGIC If step 2 fails after step 1, just run Steps A to D again: step 2 adds any store that has no current row, so nothing is lost.

# COMMAND ----------

# DBTITLE 1,One-time fix - close history gap for ST-001
# MAGIC %sql
# MAGIC -- ONE-TIME FIX (run only once)
# MAGIC -- The old ST-001 row ended at 06:41:57, but the new row started at 06:43:19.
# MAGIC -- Make the old row end exactly when the new row starts, so there is no gap.
# MAGIC UPDATE retail_catalog.silver.dim_stores
# MAGIC SET effective_end_date = TIMESTAMP '2026-11-05 06:43:19.336824'
# MAGIC WHERE store_id = 'ST-001'
# MAGIC   AND is_current = false
# MAGIC   AND store_name = 'Andheri Flagship Updated';

# COMMAND ----------

# DBTITLE 1,SCD2 Step A - set load time
# MAGIC %sql
# MAGIC -- Step A: Save the current time ONE time.
# MAGIC -- Steps C and D both use load_time, so the dates line up exactly.
# MAGIC DECLARE OR REPLACE VARIABLE load_time TIMESTAMP DEFAULT current_timestamp();
# MAGIC
# MAGIC SELECT load_time;

# COMMAND ----------

# DBTITLE 1,SCD2 Step B - preview changes
# MAGIC %sql
# MAGIC -- Step B: PREVIEW only (does not change any data)
# MAGIC -- Shows which stores are different in Bronze compared to the current Silver row.
# MAGIC -- <=> means "equal" and also treats two NULLs as equal.
# MAGIC SELECT
# MAGIC     s.store_id,
# MAGIC     t.store_name AS silver_store_name,
# MAGIC     s.store_name AS bronze_store_name,
# MAGIC     t.city       AS silver_city,
# MAGIC     s.city       AS bronze_city,
# MAGIC     CASE WHEN t.store_id IS NULL THEN 'NEW STORE' ELSE 'CHANGED' END AS change_type
# MAGIC FROM retail_catalog.bronze.dim_stores AS s
# MAGIC LEFT JOIN retail_catalog.silver.dim_stores AS t
# MAGIC     ON s.store_id = t.store_id
# MAGIC    AND t.is_current = true
# MAGIC WHERE s.store_id IS NOT NULL
# MAGIC   AND (
# MAGIC         t.store_id IS NULL
# MAGIC      OR NOT (t.store_name <=> s.store_name)
# MAGIC      OR NOT (t.city       <=> s.city)
# MAGIC      OR NOT (t.region     <=> s.region)
# MAGIC      OR NOT (t.store_type <=> s.store_type)
# MAGIC      OR NOT (t.is_active  <=> s.is_active)
# MAGIC   )
# MAGIC ORDER BY s.store_id;

# COMMAND ----------

# DBTITLE 1,SCD2 Step C - close old versions
# MAGIC %sql
# MAGIC -- Step C: Close the current row of every store that changed in Bronze.
# MAGIC MERGE INTO retail_catalog.silver.dim_stores AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM retail_catalog.bronze.dim_stores
# MAGIC     WHERE store_id IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.store_id = source.store_id
# MAGIC AND target.is_current = true
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        NOT (target.store_name <=> source.store_name)
# MAGIC     OR NOT (target.city       <=> source.city)
# MAGIC     OR NOT (target.region     <=> source.region)
# MAGIC     OR NOT (target.store_type <=> source.store_type)
# MAGIC     OR NOT (target.is_active  <=> source.is_active)
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.effective_end_date = load_time,
# MAGIC     target.is_current = false;

# COMMAND ----------

# DBTITLE 1,SCD2 Step D - insert new versions
# MAGIC %sql
# MAGIC -- Step D: Insert a new current row for every store that has no current row.
# MAGIC -- This covers: (1) stores closed in Step C, and (2) brand new stores.
# MAGIC INSERT INTO retail_catalog.silver.dim_stores (
# MAGIC     store_id,
# MAGIC     store_name,
# MAGIC     city,
# MAGIC     region,
# MAGIC     store_type,
# MAGIC     is_active,
# MAGIC     updated_at,
# MAGIC     effective_start_date,
# MAGIC     effective_end_date,
# MAGIC     is_current
# MAGIC )
# MAGIC SELECT
# MAGIC     s.store_id,
# MAGIC     s.store_name,
# MAGIC     s.city,
# MAGIC     s.region,
# MAGIC     s.store_type,
# MAGIC     s.is_active,
# MAGIC     s.updated_at,
# MAGIC     load_time                            AS effective_start_date,
# MAGIC     TIMESTAMP '9999-12-31 23:59:59'      AS effective_end_date,
# MAGIC     true                                 AS is_current
# MAGIC FROM retail_catalog.bronze.dim_stores AS s
# MAGIC LEFT JOIN retail_catalog.silver.dim_stores AS t
# MAGIC     ON s.store_id = t.store_id
# MAGIC    AND t.is_current = true
# MAGIC WHERE s.store_id IS NOT NULL
# MAGIC   AND t.store_id IS NULL;

# COMMAND ----------

# DBTITLE 1,SCD2 Check 1 - one current row per store
# MAGIC %sql
# MAGIC -- Check 1: Every store must have exactly ONE current row.
# MAGIC -- Good result: no rows returned.
# MAGIC SELECT
# MAGIC     store_id,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_rows
# MAGIC FROM retail_catalog.silver.dim_stores
# MAGIC GROUP BY store_id
# MAGIC HAVING SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) <> 1;

# COMMAND ----------

# DBTITLE 1,SCD2 Check 2 - view store history
# MAGIC %sql
# MAGIC -- Check 2: Look at the history of stores that have more than one version.
# MAGIC -- Good result: each old row's effective_end_date = the next row's effective_start_date.
# MAGIC SELECT
# MAGIC     store_id,
# MAGIC     store_name,
# MAGIC     city,
# MAGIC     effective_start_date,
# MAGIC     effective_end_date,
# MAGIC     is_current
# MAGIC FROM retail_catalog.silver.dim_stores
# MAGIC WHERE store_id IN (
# MAGIC     SELECT store_id
# MAGIC     FROM retail_catalog.silver.dim_stores
# MAGIC     GROUP BY store_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC ORDER BY store_id, effective_start_date;