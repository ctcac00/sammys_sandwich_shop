# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension: Date
# MAGIC Date spine with calendar attributes.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, explode, sequence, to_date, year, month, dayofmonth,
    dayofweek, dayofyear, weekofyear, quarter, date_format
)
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

@dlt.table(
    name="dim_date",
    comment="Date dimension with calendar attributes",
    table_properties={"quality": "gold"}
)
def dim_date():
    # Generate date spine
    date_spine = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit(DATE_SPINE_START)),
                    to_date(lit(DATE_SPINE_END))
                )
            ).alias("full_date")
        )
    )
    
    return (
        date_spine
        .select(
            (year(col("full_date")) * 10000 + 
             month(col("full_date")) * 100 + 
             dayofmonth(col("full_date"))).cast(IntegerType()).alias("date_key"),
            col("full_date"),
            year(col("full_date")).alias("year"),
            quarter(col("full_date")).alias("quarter"),
            month(col("full_date")).alias("month"),
            date_format(col("full_date"), "MMMM").alias("month_name"),
            date_format(col("full_date"), "MMM").alias("month_short"),
            weekofyear(col("full_date")).alias("week_of_year"),
            dayofmonth(col("full_date")).alias("day_of_month"),
            dayofyear(col("full_date")).alias("day_of_year"),
            dayofweek(col("full_date")).alias("day_of_week"),
            date_format(col("full_date"), "EEEE").alias("day_name"),
            date_format(col("full_date"), "E").alias("day_short"),
            when(dayofweek(col("full_date")).isin(1, 7), lit(True))
                .otherwise(lit(False)).alias("is_weekend"),
            # Fiscal year (assuming Oct start)
            when(month(col("full_date")) >= 10, year(col("full_date")) + 1)
                .otherwise(year(col("full_date"))).alias("fiscal_year"),
            when(month(col("full_date")) >= 10, quarter(col("full_date")) - 3)
                .otherwise(quarter(col("full_date")) + 1).alias("fiscal_quarter")
        )
    )
