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
partitions=["year","month","day"]
target_path=f"s3://{db_bucket}/{prefix_name}/{table_name}"
target_table_name="gold_"+table_name
target_table_path=f"{catalog}.{schema_name}.{target_table_name}"

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



