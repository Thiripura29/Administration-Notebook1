# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

dbutils.widgets.text("config_path","/Workspace/Users/thiripura40@gmail.com/Administration-Notebook1/configs/dev1.json")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

source_name="gold-administration-organization"

# COMMAND ----------

silver_organization_partition_id_to_be_processed,silver_organization_partition_to_be_processed=get_partition_info(source_name,'silver_organizations','administration')
silver_organization_predicate= " OR ".join(list (set(silver_organization_partition_to_be_processed)))
print(silver_organization_predicate)

# COMMAND ----------

#Load data
silver_organization_df=spark.sql(f"""
        select * from lakehouse_dev.administration.silver_organizations
        where {silver_organization_predicate}
        """)

display(silver_organization_df)
