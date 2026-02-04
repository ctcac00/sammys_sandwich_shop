# Databricks notebook source
# MAGIC %md
# MAGIC # Helper Functions
# MAGIC Shared utility functions for data transformations across all notebooks.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, trim, lower, upper, initcap,
    to_date, to_timestamp, coalesce, when, md5, concat_ws,
    datediff, current_date, floor, hour, minute, dayofweek,
    dayofmonth, month, year, quarter, weekofyear
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, BooleanType

# COMMAND ----------

def generate_surrogate_key(df: DataFrame, *columns: str) -> DataFrame:
    """
    Generate a surrogate key using MD5 hash of concatenated columns.
    Equivalent to dbt_utils.generate_surrogate_key()
    
    Args:
        df: Input DataFrame
        columns: Column names to include in the key
        
    Returns:
        DataFrame with new column '{first_column}_sk' containing the surrogate key
    """
    key_name = f"{columns[0]}_sk" if len(columns) == 1 else "surrogate_key"
    return df.withColumn(key_name, md5(concat_ws("||", *[col(c).cast(StringType()) for c in columns])))

# COMMAND ----------

def safe_cast_to_date(df: DataFrame, column: str, format: str = "yyyy-MM-dd") -> DataFrame:
    """
    Safely cast a column to date type, returning null for invalid values.
    Equivalent to Snowflake's try_to_date()
    """
    return df.withColumn(column, to_date(col(column), format))

def safe_cast_to_number(df: DataFrame, column: str) -> DataFrame:
    """
    Safely cast a column to double type, returning null for invalid values.
    Equivalent to Snowflake's try_to_number()
    """
    return df.withColumn(column, col(column).cast(DoubleType()))

def safe_cast_to_int(df: DataFrame, column: str) -> DataFrame:
    """
    Safely cast a column to integer type, returning null for invalid values.
    """
    return df.withColumn(column, col(column).cast(IntegerType()))

# COMMAND ----------

def clean_string(df: DataFrame, column: str, case: str = "none") -> DataFrame:
    """
    Clean a string column by trimming whitespace and optionally changing case.
    
    Args:
        df: Input DataFrame
        column: Column name to clean
        case: One of 'none', 'lower', 'upper', 'initcap'
    """
    cleaned = trim(col(column))
    
    if case == "lower":
        cleaned = lower(cleaned)
    elif case == "upper":
        cleaned = upper(cleaned)
    elif case == "initcap":
        cleaned = initcap(cleaned)
    
    return df.withColumn(column, cleaned)

# COMMAND ----------

def string_to_boolean(df: DataFrame, column: str, true_values: list = None) -> DataFrame:
    """
    Convert a string column to boolean based on specified true values.
    
    Args:
        df: Input DataFrame
        column: Column name to convert
        true_values: List of string values that should be True (default: ['TRUE', 'YES', '1', 'true', 'yes'])
    """
    if true_values is None:
        true_values = ['TRUE', 'YES', '1', 'true', 'yes', 'True', 'Yes']
    
    return df.withColumn(
        column,
        when(upper(trim(col(column))).isin([v.upper() for v in true_values]), lit(True))
        .otherwise(lit(False))
    )

# COMMAND ----------

def add_metadata_columns(df: DataFrame, source_file: str) -> DataFrame:
    """
    Add standard metadata columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        source_file: Name of the source file
    """
    return df.withColumn("_loaded_at", current_timestamp()) \
             .withColumn("_source_file", lit(source_file))

# COMMAND ----------

def calculate_age(df: DataFrame, birth_date_col: str, as_of_date_col: str = None) -> DataFrame:
    """
    Calculate age in years from a birth date column.
    
    Args:
        df: Input DataFrame
        birth_date_col: Name of birth date column
        as_of_date_col: Optional column to calculate age as of (default: current_date)
    """
    if as_of_date_col:
        age_calc = floor(datediff(col(as_of_date_col), col(birth_date_col)) / 365.25)
    else:
        age_calc = floor(datediff(current_date(), col(birth_date_col)) / 365.25)
    
    return df.withColumn("age", age_calc.cast(IntegerType()))

# COMMAND ----------

def calculate_tenure_days(df: DataFrame, start_date_col: str, as_of_date_col: str = None) -> DataFrame:
    """
    Calculate tenure in days from a start date column.
    """
    if as_of_date_col:
        tenure_calc = datediff(col(as_of_date_col), col(start_date_col))
    else:
        tenure_calc = datediff(current_date(), col(start_date_col))
    
    return df.withColumn("tenure_days", tenure_calc)

# COMMAND ----------

def create_date_key(df: DataFrame, date_col: str, key_col: str = "date_key") -> DataFrame:
    """
    Create a date key in YYYYMMDD integer format.
    """
    return df.withColumn(
        key_col,
        (year(col(date_col)) * 10000 + month(col(date_col)) * 100 + dayofmonth(col(date_col))).cast(IntegerType())
    )

def create_time_key(df: DataFrame, time_col: str, key_col: str = "time_key") -> DataFrame:
    """
    Create a time key in HHMM integer format.
    """
    return df.withColumn(
        key_col,
        (hour(col(time_col)) * 100 + minute(col(time_col))).cast(IntegerType())
    )

# COMMAND ----------

def write_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """
    Write DataFrame to a Delta table with standard options.
    
    Args:
        df: DataFrame to write
        table_name: Fully qualified table name (catalog.schema.table)
        mode: Write mode ('overwrite' or 'append')
    """
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"✓ Written {df.count()} rows to {table_name}")

def create_or_replace_view(df: DataFrame, view_name: str, spark: SparkSession):
    """
    Create or replace a view from a DataFrame.
    
    Args:
        df: DataFrame to create view from
        view_name: Fully qualified view name (catalog.schema.view)
        spark: SparkSession
    """
    df.createOrReplaceTempView("temp_view")
    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM temp_view")
    print(f"✓ Created view {view_name}")
