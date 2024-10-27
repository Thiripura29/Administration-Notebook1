# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

dbutils.widgets.text("config_path","../configs/dev1.json")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

source_name="gold-administration-organization"

# COMMAND ----------


