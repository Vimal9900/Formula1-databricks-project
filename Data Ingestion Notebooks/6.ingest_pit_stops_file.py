# Databricks notebook source
#Storage Mounting on different container by callling another notebook(eg. raw,processed,presentation,demo)
#1. raw container mount is needed for reading the input data
dbutils.notebook.run("storage_mount",120,{"storage" : "formula1storagaccount","container" : "raw"})
#2. processed container mount is needed for writing the proccesed/output data 
dbutils.notebook.run("storage_mount",120,{"storage" : "formula1storagaccount","container" : "processed"})
# display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType,DateType
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat
# Below 2 lines are not necessarily in databricks
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("Formula1").getOrCreate()

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

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/formula1storagaccount/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#Write to output to processed container in parquet format
final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/pit_stops")

# COMMAND ----------


