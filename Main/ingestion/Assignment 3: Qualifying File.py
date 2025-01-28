# Databricks notebook source
# MAGIC %md
# MAGIC ## READ DATA

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), True), 
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True), 
                                       StructField("constructorId", IntegerType(), True), 
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("abfss://raw@formula1project2025.dfs.core.windows.net/qualifying")

# specify the whole folder
# or use wildcard raw/lap_times/lap_times_split*.csv

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRANSFORM DATA

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnsRenamed({"qualifyId": "qualify_id", "raceId": "race_id", "driverId": "driver_id", "constructorId": "constructor_id"})

# COMMAND ----------

final_df = final_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA OUTPUT

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://processed@formula1project2025.dfs.core.windows.net/qualifying")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@formula1project2025.dfs.core.windows.net/qualifying"))
