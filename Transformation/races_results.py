# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time") 

# COMMAND ----------

qualifying_df = spark.read.parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

drivers_join = qualifying_df.join(drivers_df,drivers_df.driver_id == qualifying_df.driver_id,"inner")
