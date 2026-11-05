# Databricks notebook source
from pyspark.sql import functions as F

# Read the final Silver table
silver_df = spark.table("retail_catalog.silver.sales")

# Verify the source data
print("Silver records:", silver_df.count())

display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # **Calculate total revenue by region**

# COMMAND ----------

# Calculate total revenue for each region
region_revenue_df = (
    silver_df
    .groupBy("region")
    .agg(
        F.sum("amount").alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(region_revenue_df)

# COMMAND ----------

region_revenue_df = (
    silver_df
    .groupBy("region")
    .agg(
        F.sum("amount").alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(region_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate the revenue calculation

# COMMAND ----------

# Calculate revenue directly from Silver
total_sales_revenue = silver_df.agg(
    F.sum("amount").alias("total")
).first()["total"]

# Calculate revenue from regional aggregation
regional_revenue = region_revenue_df.agg(
    F.sum("total_revenue").alias("total")
).first()["total"]

print("Total Silver revenue:", total_sales_revenue)
print("Total regional revenue:", regional_revenue)
print("Revenue matches:", total_sales_revenue == regional_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate revenue by product category

# COMMAND ----------

category_revenue_df = (
    silver_df
    .groupBy("category")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.countDistinct("sale_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(category_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate category aggregation

# COMMAND ----------

# Revenue validation
category_revenue = category_revenue_df.agg(
    F.sum("total_revenue")
).first()[0]

# Order validation
category_orders = category_revenue_df.agg(
    F.sum("total_orders")
).first()[0]

# Units validation
category_units = category_revenue_df.agg(
    F.sum("total_units_sold")
).first()[0]

print("Category revenue:", category_revenue)
print("Category orders:", category_orders)
print("Category units:", category_units)

print("Revenue matches:", category_revenue == total_sales_revenue)
print("Orders match:", category_orders == silver_df.select("sale_id").distinct().count())
print("Units match:", category_units == silver_df.agg(F.sum("quantity")).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate revenue by payment mode

# COMMAND ----------

payment_revenue_df = (
    silver_df
    .groupBy("payment_mode")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.countDistinct("sale_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(payment_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate payment-mode aggregation

# COMMAND ----------

# Calculate payment-mode totals
payment_revenue = payment_revenue_df.agg(
    F.sum("total_revenue")
).first()[0]

payment_orders = payment_revenue_df.agg(
    F.sum("total_orders")
).first()[0]

payment_units = payment_revenue_df.agg(
    F.sum("total_units_sold")
).first()[0]

# Compare against Silver
print("Payment revenue:", payment_revenue)
print("Payment orders:", payment_orders)
print("Payment units:", payment_units)

print("Revenue matches:", payment_revenue == total_sales_revenue)
print(
    "Orders match:",
    payment_orders == silver_df.select("sale_id").distinct().count()
)
print(
    "Units match:",
    payment_units == silver_df.agg(F.sum("quantity")).first()[0]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate revenue by city

# COMMAND ----------

city_revenue_df = (
    silver_df
    .groupBy("city")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.countDistinct("sale_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("store_id").alias("total_stores")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(city_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate city-level aggregation

# COMMAND ----------

# Calculate city-level totals
city_revenue = city_revenue_df.agg(
    F.sum("total_revenue")
).first()[0]

city_orders = city_revenue_df.agg(
    F.sum("total_orders")
).first()[0]

city_units = city_revenue_df.agg(
    F.sum("total_units_sold")
).first()[0]

print("City revenue:", city_revenue)
print("City orders:", city_orders)
print("City units:", city_units)

print("Revenue matches:", city_revenue == total_sales_revenue)

print(
    "Orders match:",
    city_orders == silver_df.select("sale_id").distinct().count()
)

print(
    "Units match:",
    city_units == silver_df.agg(F.sum("quantity")).first()[0]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate overall business KPIs

# COMMAND ----------

overall_kpis_df = silver_df.agg(
    F.sum("amount").alias("total_revenue"),
    F.countDistinct("sale_id").alias("total_orders"),
    F.sum("quantity").alias("total_units_sold"),
    (
        F.sum("amount") /
        F.countDistinct("sale_id")
    ).alias("average_order_value"),
    (
        F.sum(
            F.when(
                F.col("payment_mode") == "UPI",
                F.col("amount")
            ).otherwise(0)
        ) / F.sum("amount") * 100
    ).alias("upi_revenue_percentage")
)

display(overall_kpis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format the overall KPIs

# COMMAND ----------

overall_kpis_df = silver_df.agg(
    F.round(F.sum("amount"), 2).alias("total_revenue"),

    F.countDistinct("sale_id").alias("total_orders"),

    F.sum("quantity").alias("total_units_sold"),

    F.round(
        F.sum("amount") / F.countDistinct("sale_id"),
        2
    ).alias("average_order_value"),

    F.round(
        F.sum(
            F.when(
                F.col("payment_mode") == "UPI",
                F.col("amount")
            ).otherwise(0)
        ) / F.sum("amount") * 100,
        2
    ).alias("upi_revenue_percentage")
)

display(overall_kpis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the five Gold tables

# COMMAND ----------

# Save regional revenue
region_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.gold.revenue_by_region")

# Save category revenue
category_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.gold.revenue_by_category")

# Save payment-mode revenue
payment_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.gold.revenue_by_payment_mode")

# Save city revenue
city_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.gold.revenue_by_city")

# Save overall business KPIs
overall_kpis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.gold.overall_business_kpis")

print("All five Gold tables saved successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail_catalog.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify persisted Gold data

# COMMAND ----------

# Read the saved Gold tables again
gold_region_df = spark.table(
    "retail_catalog.gold.revenue_by_region"
)

gold_category_df = spark.table(
    "retail_catalog.gold.revenue_by_category"
)

gold_payment_df = spark.table(
    "retail_catalog.gold.revenue_by_payment_mode"
)

gold_city_df = spark.table(
    "retail_catalog.gold.revenue_by_city"
)

gold_overall_df = spark.table(
    "retail_catalog.gold.overall_business_kpis"
)

# Verify record counts
print("Region rows:", gold_region_df.count())
print("Category rows:", gold_category_df.count())
print("Payment mode rows:", gold_payment_df.count())
print("City rows:", gold_city_df.count())
print("Overall KPI rows:", gold_overall_df.count())

# Inspect the saved overall KPIs
display(gold_overall_df)

# COMMAND ----------

spark.table("retail_catalog.gold.overall_business_kpis").show(
    truncate=False
)