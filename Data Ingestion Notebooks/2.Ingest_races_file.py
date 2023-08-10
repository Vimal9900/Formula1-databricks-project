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

#schema for circuits file
races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

#reading the circuit file
races_df = spark.read \
        .option("header",True) \
        .schema(races_schema) \
        .csv("/mnt/formula1storagaccount/raw/races.csv")


# COMMAND ----------

#circuit_df.show(truncate=False)
display(races_df)

# COMMAND ----------

#Add ingestion date and race_timestamp to the dataframe
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

#Select only the columns required & rename as required
races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1storagaccount/processed/races/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1storagaccount/processed/races

# COMMAND ----------


