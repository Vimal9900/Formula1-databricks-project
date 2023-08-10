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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/formula1storagaccount/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1storagaccount/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1storagaccount/processed/qualifying'))

# COMMAND ----------


