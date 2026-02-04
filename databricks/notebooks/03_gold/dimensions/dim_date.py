# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Date
# MAGIC Generates a date spine with various date attributes.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_date.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, year, quarter, month, dayofmonth, dayofweek, weekofyear,
    date_format, lit, current_timestamp, concat, expr, to_date
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Date Spine

# COMMAND ----------

# Generate date range from 2020-01-01 to 2030-12-31
df_dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{DATE_SPINE_START}'), 
        to_date('{DATE_SPINE_END}'), 
        interval 1 day
    )) as full_date
""")

print(f"Generated {df_dates.count()} dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Date Attributes

# COMMAND ----------

dim_date = df_dates.select(
    # Surrogate key (YYYYMMDD format)
    (year(col("full_date")) * 10000 + month(col("full_date")) * 100 + dayofmonth(col("full_date"))).cast(IntegerType()).alias("date_key"),
    
    col("full_date"),
    
    # Year attributes
    year(col("full_date")).alias("year"),
    quarter(col("full_date")).alias("quarter"),
    concat(lit("Q"), quarter(col("full_date"))).alias("quarter_name"),
    
    # Month attributes
    month(col("full_date")).alias("month"),
    date_format(col("full_date"), "MMMM").alias("month_name"),
    date_format(col("full_date"), "MMM").alias("month_abbr"),
    
    # Week attributes
    weekofyear(col("full_date")).alias("week_of_year"),
    
    # Day attributes
    dayofmonth(col("full_date")).alias("day_of_month"),
    dayofweek(col("full_date")).alias("day_of_week"),
    date_format(col("full_date"), "EEEE").alias("day_name"),
    date_format(col("full_date"), "EEE").alias("day_abbr"),
    
    # Flags (Spark: Sunday=1, Saturday=7)
    dayofweek(col("full_date")).isin(1, 7).alias("is_weekend"),
    lit(False).alias("is_holiday"),
    lit(None).cast("string").alias("holiday_name"),
    
    # Fiscal year (assuming calendar year = fiscal year)
    year(col("full_date")).alias("fiscal_year"),
    quarter(col("full_date")).alias("fiscal_quarter"),
    
    # Metadata
    current_timestamp().alias("_created_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_date")
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_date.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
