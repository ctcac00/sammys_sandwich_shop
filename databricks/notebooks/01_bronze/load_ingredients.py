# Databricks notebook source
# MAGIC %md
# MAGIC # Load Ingredients to Bronze
# MAGIC Loads ingredient data from CSV into the bronze layer.

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load CSV Data

# COMMAND ----------

csv_path = f"{DATA_PATH}/{CSV_FILES['ingredients']}"
print(f"Loading from: {csv_path}")

df = spark.read.csv(csv_path, header=True, inferSchema=True)
print(f"Loaded {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Metadata Columns

# COMMAND ----------

df = df.withColumn("_loaded_at", current_timestamp()) \
       .withColumn("_source_file", lit(CSV_FILES['ingredients']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Table

# COMMAND ----------

table_name = bronze_table("ingredients")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ Written {df.count()} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Data

# COMMAND ----------

display(spark.table(table_name).limit(10))
