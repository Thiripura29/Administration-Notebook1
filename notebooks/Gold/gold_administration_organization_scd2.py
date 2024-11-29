# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

dbutils.widgets.text("config_path","../../configs/dev1.json")

# COMMAND ----------

import json
config_path=dbutils.widgets.get("config_path")
config=load_config(config_path)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

source_name="gold-administration-organization"

# COMMAND ----------

db_bucket=config["bucket-name"]
prefix_name=config["gold-prefix"]
table_name="organizations"
catalog=config["catalog-name"]
schema_name=config["schema-name"]
target_path=f"s3://{db_bucket}/{prefix_name}/{table_name}"
target_table_name="gold_"+table_name
target_table_path=f"{catalog}.{schema_name}.{target_table_name}"
partitions=["year","month","day"]

# COMMAND ----------

display(target_table_name)

# COMMAND ----------

silver_organization_partition_id_to_be_processed,silver_organization_partition_to_be_processed=get_partition_info(source_name,'silver_organizations','administration')
silver_organization_predicate= " OR ".join(list (set(silver_organization_partition_to_be_processed)))
print(silver_organization_predicate)

# COMMAND ----------

#Load data
silver_organization_df=spark.sql(f"""
        select * ,case when rank >1 then TRUE else FALSE end as flag from
        (
          select *, dense_rank() over (partition by id order by Day,Month,Year desc) as rank from
              (select * from lakehouse_dev.administration.silver_organizations
                where {silver_organization_predicate}
              )
        )
        """)

display(silver_organization_df)

# COMMAND ----------

table_list=spark.sql("show tables in lakehouse_dev.administration").collect()
display(table_list)

# COMMAND ----------

print(target_table_name)
does_table_exist=False
for table in table_list:
  if table[1] == target_table_name:
    does_table_exist = True
    break
print(does_table_exist)

# COMMAND ----------

col_drop=["rank"]
silver_organization_df=silver_organization_df.drop(*col_drop)
display(silver_organization_df)

# COMMAND ----------

if not does_table_exist:
  silver_organization_df.write.format('delta').option("path",target_path).saveAsTable(target_table_path)


# COMMAND ----------

from pyspark.sql.functions import *
gold_organization_df=spark.sql(f"select * from {target_table_path}")
display(gold_organization_df)

# COMMAND ----------

from pyspark.sql.functions import *
if does_table_exist:
  gold_organization_df=spark.sql(f"select * from {target_table_path}")
  common_records_df=silver_organization_df.join(gold_organization_df,gold_organization_df['Id']==silver_organization_df['Id'])
  #display(common_records_df)
  staged_updates=common_records_df.select(lit(None).alias('Mergekey'),silver_organization_df['*'])\
  .union(\
    silver_organization_df.select(col('Id').alias('Mergekey'),'*')\
      )
            
  display(staged_updates.orderBy("Id"))
  staged_updates.createTempView("staged_updates")
  spark.sql(f"""
            MERGE INTO {target_table_path} t1
            USING staged_updates t2
            on t1.Id=t2.Mergekey
            when matched then update set flag=FALSE
            when not matched then insert *
            """)


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
    "partition_column_info":""
    },audit_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.administration.pipeline_audit_log_table

# COMMAND ----------

#Apply transformations to the good records
spark.sql(f"""
          update lakehouse_dev.administration.pipeline_audit_log_table
          set processed_status_info_array=
          case when processed_status_info_array is null then array('{source_name}')
          else
           array_union(processed_status_info_array,array('{source_name}')) end
          where audit_id in ({','.join([f"'{item}'" for item in silver_organization_partition_id_to_be_processed])})
          """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.administration.pipeline_audit_log_table
