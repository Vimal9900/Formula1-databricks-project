# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest circuits.csv file

# COMMAND ----------

#Storage Mounting on different container by callling another notebook(eg. raw,processed,presentation,demo)
#1. raw container mount is needed for reading the input data
dbutils.notebook.run("storage_mount",120,{"storage" : "formula1storagaccount","container" : "raw"})
#2. processed container mount is needed for writing the proccesed/output data 
dbutils.notebook.run("storage_mount",120,{"storage" : "formula1storagaccount","container" : "processed"})
# display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType
from pyspark.sql.functions import col,current_timestamp
# Below 2 lines are not necessarily in databricks
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("Formula1").getOrCreate()

# COMMAND ----------

#schema for circuits file
circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

#reading the circuit file
circuits_df = spark.read \
        .option("header",True) \
        .schema(circuits_schema) \
        .csv("/mnt/formula1storagaccount/raw/circuits.csv")



# COMMAND ----------

#circuit_df.show(truncate=False)
display(circuits_df)

# COMMAND ----------

#selecting the required colums and renaming it

circuits_selected_df = circuits_df.select(col("circuitId").alias("circuit_id"), 
                                          col("circuitRef").alias("circuit_ref"), 
                                          col("name"),
                                           col("location"), 
                                          col("country"), 
                                          col("lat").alias("latitude"), 
                                          col("lng").alias("longitude"), 
                                          col("alt").alias("altitude"))

# COMMAND ----------

# Add ingestion date to the dataframe

circuits_final_df = circuits_selected_df.withColumn("ingestion_date",current_timestamp())


# COMMAND ----------


#Writing data to datalake as parquet
circuits_final_df.write \
        .mode("overwrite") \
        .parquet("/mnt/formula1storagaccount/processed/circuits")


# COMMAND ----------

display(spark.read.parquet("/mnt/formula1storagaccount/processed/circuits"))

# COMMAND ----------

#unmouting the mounted container
# dbutils.fs.unmount("/mnt/formula1storagaccount/raw")
# dbutils.fs.unmount("/mnt/formula1storagaccount/processed")
