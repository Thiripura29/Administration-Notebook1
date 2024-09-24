# Databricks notebook source
# MAGIC %run ./utils/utils

# COMMAND ----------

dbutils.widgets.text("config_path","../configs/dev1.json")

# COMMAND ----------

import json
config_path=dbutils.widgets.get("config_path")
config=load_config(config_path)


# COMMAND ----------

print(config)

# COMMAND ----------

print(config_path)

# COMMAND ----------

database_config=config["database-config"]
health_config=database_config["healthcare"]


# COMMAND ----------

url=health_config["url"]
jdbc_credentials=get_jdbc_credentials_from_scope(health_config["scope_name"])
username=jdbc_credentials["user_name"]
password=jdbc_credentials["password"]
driver_name=health_config["driver"]

# COMMAND ----------

options= {
"url":f"{url}/healthcare",
"dbtable":"organizations",
"user":username,
"password":password
}
df=spark.read.format("jdbc").options(**options).load()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import day, month, year, lit,col
columns=df.columns
transform_df=df\
                .withColumn("audit_source_record_id", F.hash(F.concat_ws("||", *columns)))\
                .withColumn("audit_ingestion_timestamp", F.current_timestamp())

transform_df=transform_df\
               .withColumn("Day",day(col("audit_ingestion_timestamp")))\
               .withColumn("Month",month(col("audit_ingestion_timestamp")))\
               .withColumn("Year",year(col("audit_ingestion_timestamp")))\
               .withColumn("department",lit("administration"))
display(transform_df)


# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

db_bucket=config["bucket-name"]
bronze_prefix=config["bronze-prefix"]
table_name="organizations"
catalog=config["catalog-name"]
schema_name=config["schema-name"]

target_path=f"s3://{db_bucket}/{bronze_prefix}/{table_name}"
target_table_path=f"{catalog}.{schema_name}.bronze_{table_name}"

transform_df\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .partitionBy("year","month","day")\
    .option("path",target_path)\
    .saveAsTable(target_table_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.administration.bronze_organizations
