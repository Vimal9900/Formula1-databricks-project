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

#Read the JSON file using the spark dataframe reader
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

#reading the constructors file
constructor_df = spark.read \
        .schema(constructors_schema) \
        .json("/mnt/formula1storagaccount/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# Drop unwanted columns from the dataframeconstructor_df
constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

#  Rename columns and add ingestion date
constructor_final_df = constructor_dropped_df.withColumnRenamed('constructorId','constructor_id') \
                                            .withColumnRenamed('constructorRef','constructor_ref') \
                                            .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet("/mnt/formula1storagaccount/processed/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1storagaccount/processed/constructors"))

# COMMAND ----------


