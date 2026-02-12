# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Time
# MAGIC Time of day dimension.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import IntegerType

# COMMAND ----------

@dlt.table(
    name="dim_time",
    comment="Time of day dimension",
    table_properties={"quality": "gold"}
)
def dim_time():
    # Generate time spine (every minute)
    time_spine = (
        spark.range(24 * 60)
        .select(
            (col("id") / 60).cast(IntegerType()).alias("hour"),
            (col("id") % 60).cast(IntegerType()).alias("minute")
        )
    )
    
    return (
        time_spine
        .select(
            (col("hour") * 100 + col("minute")).cast(IntegerType()).alias("time_key"),
            col("hour"),
            col("minute"),
            when(col("hour") < 12, "AM").otherwise("PM").alias("am_pm"),
            when(col("hour") == 0, 12)
                .when(col("hour") <= 12, col("hour"))
                .otherwise(col("hour") - 12).alias("hour_12"),
            when(col("hour").between(6, 10), "Breakfast")
                .when(col("hour").between(11, 14), "Lunch")
                .when(col("hour").between(15, 17), "Afternoon")
                .when(col("hour").between(18, 21), "Dinner")
                .otherwise("Late Night").alias("time_period"),
            when(col("hour").between(11, 13), lit(True))
                .otherwise(lit(False)).alias("is_lunch_rush"),
            when(col("hour").between(17, 19), lit(True))
                .otherwise(lit(False)).alias("is_dinner_rush")
        )
    )
