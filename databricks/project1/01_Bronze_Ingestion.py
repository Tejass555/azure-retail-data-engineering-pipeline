# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import *

# COMMAND ----------

# Source ADLS location

source_path = (
    "abfss://landing@project1adls1.dfs.core.windows.net/raw_sales/"
)

# Schema tracking location

schema_location = (
    "abfss://landing@project1adls1.dfs.core.windows.net/_schemas/raw_sales/"
)

# Checkpoint location

checkpoint_location = (
    "abfss://landing@project1adls1.dfs.core.windows.net/_checkpoints/raw_sales/"
)

# Destination Bronze table
output_table = "retail_catalog.bronze.raw_sales"

print("Source:", source_path)
print("Schema:", schema_location)
print("Checkpoint:", checkpoint_location)
print("output:", output_table)

# COMMAND ----------

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
)

# COMMAND ----------

# DBTITLE 1,Cell 4
spark.table(output_table).count() if spark.catalog.tableExists(output_table) else 0

# COMMAND ----------

# DBTITLE 1,Write stream to Bronze table
from pyspark.sql.functions import col

df_bronze = (df_stream
             .withColumn("_ingested_at", current_timestamp())
             .withColumn("_source_file", col("_metadata.file_path")))

query = (df_bronze.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", checkpoint_location)
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(output_table))

query.awaitTermination()

print("Bronze ingestion completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC ALTER TABLE retail_catalog.bronze.raw_sales
# MAGIC ALTER COLUMN _source_file TYPE STRING;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT sale_id) AS unique_sales,
# MAGIC     COUNT(*) - COUNT(DISTINCT sale_id) AS duplicate_rows
# MAGIC FROM retail_catalog.bronze.raw_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sale_id,
# MAGIC     updated_at,
# MAGIC     _ingested_at,
# MAGIC     _source_file
# MAGIC FROM retail_catalog.bronze.raw_sales
# MAGIC LIMIT 10;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# 1. Define source and destination paths
source_path = "abfss://landing@project1adls1.dfs.core.windows.net/dim_stores/"

schema_path = "abfss://landing@project1adls1.dfs.core.windows.net/_schemas/dim_stores/"

checkpoint_path = "abfss://landing@project1adls1.dfs.core.windows.net/_checkpoints/dim_stores/"

output_table = "retail_catalog.bronze.dim_stores"

# 2. Read Parquet files using Auto Loader
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
)


# 3. Add ingestion metadata
df_bronze = (
    df_stream
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)


# 4. Write data into Bronze Delta table
query = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(output_table)
)

query.awaitTermination()

print("Dim stores ingestion completed successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT store_id) AS unique_stores
# MAGIC FROM retail_catalog.bronze.dim_stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_catalog.bronze.dim_stores;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# 1. Define paths
source_path = "abfss://landing@project1adls1.dfs.core.windows.net/dim_products/"
schema_path = "abfss://landing@project1adls1.dfs.core.windows.net/_schemas/dim_products/"
checkpoint_path = "abfss://landing@project1adls1.dfs.core.windows.net/_checkpoints/dim_products/"
output_table = "retail_catalog.bronze.dim_products"

# 2. Read source files using Auto Loader
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
)

# 3. Add metadata
df_bronze = (
    df_stream
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# 4. Write into Bronze
query = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(output_table)
)

query.awaitTermination()

print("Dim products ingestion completed successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT product_id) AS unique_products,
# MAGIC     COUNT(*) - COUNT(DISTINCT product_id) AS duplicate_rows
# MAGIC FROM retail_catalog.bronze.dim_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'raw_sales' AS table_name, COUNT(*) AS total_rows
# MAGIC FROM retail_catalog.bronze.raw_sales
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_stores', COUNT(*)
# MAGIC FROM retail_catalog.bronze.dim_stores
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_products', COUNT(*)
# MAGIC FROM retail_catalog.bronze.dim_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'raw_sales' AS table_name,
# MAGIC        COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT sale_id) AS unique_records
# MAGIC FROM retail_catalog.bronze.raw_sales
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_stores', COUNT(*), COUNT(DISTINCT store_id)
# MAGIC FROM retail_catalog.bronze.dim_stores
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_products', COUNT(*), COUNT(DISTINCT product_id)
# MAGIC FROM retail_catalog.bronze.dim_products;

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT sale_id) AS unique_sales,
    SUM(CASE WHEN sale_id = 'TXN-10033' THEN 1 ELSE 0 END) AS test_record_count
FROM retail_catalog.bronze.raw_sales
""").show()