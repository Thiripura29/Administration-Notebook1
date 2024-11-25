# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

dbutils.widgets.text("config_path","/Workspace/Users/thiripura40@gmail.com/Administration-Notebook1/configs/dev1.json")

# COMMAND ----------

import json
config_path=dbutils.widgets.get("config_path")
#config_path = '/Workspace/Users/thiripura40@gmail.com/Administration-Notebook1/configs/dev1.json'
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

# MAGIC %pip install mysql-connector-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Databricks Shell") \
    .config("spark.jars", r"C:\Program Files\mysql-connector-j-8.4.0.jar") \
    .getOrCreate()

# COMMAND ----------

display(spark)

# COMMAND ----------

print(spark.conf.get("spark.app.name"))

# COMMAND ----------

spark.stop()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark._jvm.com.mysql.cj.jdbc.Driver 

# COMMAND ----------

options= {
"url":f"{url}/healthcare",
"driver": driver_name,
"dbtable":"patients",
"user":username,
"password":password

}
try :
  df=spark.read.format('jdbc').options(**options).load()
except Exception as e:
    print(f"Error loading data: {e}")
#df.show()

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
table_name="patients"
catalog=config["catalog-name"]
schema_name=config["schema-name"]
partitions=["year","month","day"]
target_path=f"s3://{db_bucket}/{bronze_prefix}/{table_name}"
target_table_name="bronze_"+table_name
target_table_path=f"{catalog}.{schema_name}.{target_table_name}"

transform_df\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .partitionBy(*partitions)\
    .option("path",target_path)\
    .saveAsTable(target_table_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.administration.bronze_organizations

# COMMAND ----------

df=spark.sql("select * from lakehouse_dev.administration.bronze_organizations")  
display(df)

# COMMAND ----------

partitions_info=identify_partitions_predicate(transform_df,partitions)
display(partitions_info)

# COMMAND ----------

audit_table_name=f"{catalog}.{schema_name}.{config['audit_table']}"
make_audit_entry(
{
    "sink_name":f"{target_table_name}_sink",
    "data_load_trype":"incremental",
    "db_schema_name":schema_name,
    "db_table_name":target_table_name,
    "data_storage_path":target_path,
    "timestamp_or_id_column_name":"",
    "last_processed_timestamp_or_id_column_value":"",
    "partition_column_info":partitions_info
},audit_table_name)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.administration.pipeline_audit_log_table
