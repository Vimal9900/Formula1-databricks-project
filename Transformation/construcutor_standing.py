# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time") 

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

display(results_df)

# COMMAND ----------

constructors_result_df = results_df.join(constructors_df,constructors_df.constructor_id == results_df.constructor_id,"inner").select( constructors_df["constructor_id"], "nationality","team","position", "points")


# COMMAND ----------

rdd= constructors_result_df.filter("team =  'Red Bull'")
display(rdd)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col,desc,asc


final_result = constructors_result_df.groupBy("team") \
.agg(sum("points").alias("points"))

# COMMAND ----------

display(final_result.filter("team =  'Red Bull'"))

# COMMAND ----------


