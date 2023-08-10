# Databricks notebook source
#Storage Mounting on different container by callling another notebook(eg. raw,processed,presentation,demo)
#1. raw container mount is needed for reading the input data
dbutils.notebook.run("storage_mount",120,{"storage" : "formula1storagaccount","container" : "raw"})
#2. processed container mount is needed for writing the proccesed/output data 
dbutils.notebook.run("storage_mount",120,{"storage" : "formula1storagaccount","container" : "processed"})
# display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType,DateType,FloatType
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat
# Below 2 lines are not necessarily in databricks
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("Formula1").getOrCreate()

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json("/mnt/formula1storagaccount/raw/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# Rename columns and add new columns
results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

#Drop the unwanted column
results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/formula1storagaccount/processed/results")

# COMMAND ----------


