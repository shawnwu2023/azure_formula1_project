# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# additional option to set multiline = true

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("abfss://raw@formula1project2025.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

pit_stops_df = spark.read \
.option("multiLine", True) \
.json("abfss://raw@formula1project2025.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://processed@formula1project2025.dfs.core.windows.net/pit_stops")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@formula1project2025.dfs.core.windows.net/pit_stops"))
