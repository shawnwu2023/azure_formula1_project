# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# use widgets

dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# use lit to convert into a column type to add to a data frame
# the column would have the value of the lit() function populated
from pyspark.sql.functions import lit
lit(data_source)

# COMMAND ----------

# MAGIC %run ../includes/configurations

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# Use the cluster scoped approach
# display(dbutils.fs.ls("abfss://raw@formula1project2025.dfs.core.windows.net"))
#circuits_df = spark.read.options(header=True, inferSchema=True)\
    #.csv("abfss://raw@formula1project2025.dfs.core.windows.net/circuits.csv")

circuits_df = spark.read.options(header=True, inferSchema=True)\
    .csv(f"{raw_folder_path}/circuits.csv")

display(circuits_df)
#circuits_df.describe().show()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.csv("abfss://raw@formula1project2025.dfs.core.windows.net/circuits.csv", schema=circuits_schema)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
# circuits_df.select("id")
# circuits_df["id"]
# circuits_df.id

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# withColumn to add a column with values
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed@formula1project2025.dfs.core.windows.net/circuits")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@formula1project2025.dfs.core.windows.net/circuits", mode="overwrite"))

# COMMAND ----------


