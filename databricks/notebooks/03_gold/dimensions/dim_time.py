# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Time
# MAGIC Generates time of day attributes for each minute.
# MAGIC 
# MAGIC Mirrors: `dbt/models/marts/dimensions/dim_time.sql`

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, current_timestamp, expr, concat, lpad
)
from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Time Dimension

# COMMAND ----------

# Generate all hour/minute combinations (24 hours * 60 minutes = 1440 rows)
df_times = spark.sql("""
    SELECT 
        h.hour_24,
        m.minute
    FROM (SELECT explode(sequence(0, 23)) as hour_24) h
    CROSS JOIN (SELECT explode(sequence(0, 59)) as minute) m
""")

print(f"Generated {df_times.count()} time slots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Time Attributes

# COMMAND ----------

dim_time = df_times.select(
    # Surrogate key (HHMM format)
    (col("hour_24") * 100 + col("minute")).cast(IntegerType()).alias("time_key"),
    
    # Full time as string (HH:MM:SS)
    concat(
        lpad(col("hour_24").cast(StringType()), 2, "0"),
        lit(":"),
        lpad(col("minute").cast(StringType()), 2, "0"),
        lit(":00")
    ).alias("full_time"),
    
    # Hour attributes
    col("hour_24"),
    when(col("hour_24") == 0, 12)
    .when(col("hour_24") > 12, col("hour_24") - 12)
    .otherwise(col("hour_24")).alias("hour_12"),
    col("minute"),
    when(col("hour_24") < 12, "AM").otherwise("PM").alias("am_pm"),
    
    # Time period
    when(col("hour_24").between(6, 10), "Breakfast")
    .when(col("hour_24").between(11, 14), "Lunch")
    .when(col("hour_24").between(15, 17), "Afternoon")
    .when(col("hour_24").between(18, 21), "Dinner")
    .otherwise("Late Night").alias("time_period"),
    
    # Peak hour flag (lunch and dinner rush)
    (col("hour_24").between(11, 13) | col("hour_24").between(17, 19)).alias("is_peak_hour"),
    
    # Metadata
    current_timestamp().alias("_created_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

table_name = gold_table("dim_time")
dim_time.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {dim_time.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
