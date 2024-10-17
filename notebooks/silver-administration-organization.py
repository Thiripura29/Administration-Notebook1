# Databricks notebook source
# MAGIC %sql
# MAGIC alter table lakehouse_dev.administration.pipeline_audit_log_table
# MAGIC add columns (processed_status_info_array array<string>);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from lakehouse_dev.administration.pipeline_audit_log_table 

# COMMAND ----------



# COMMAND ----------

#go and check the aduit tables to get the unprocessed partitions

def get_partition_info(source_name, db_table_name,db_schema_name):
    partition_info_list=spark.sql(f"""
          select audit_id,partition_column_info
          from lakehouse_dev.administration.pipeline_audit_log_table
          where db_table_name='{db_table_name}' and db_schema_name='{db_schema_name}'
          and (not array_contains(processed_status_info_array,'{source_name}') or processed_status_info_array is null)
          order by created_date desc
          """).collect()
    partition_to_be_processed=[partition[1] for partition in partition_info_list]
    partition_id_to_be_processed=[partition[0] for partition in partition_info_list]
    #print(partition_to_be_processed)
    #print(partition_id_to_be_processed)
    return(partition_id_to_be_processed,partition_to_be_processed)

# COMMAND ----------

source_name="silver-administration-organization"

# COMMAND ----------

bronze_organization_partition_id_to_be_processed,bronze_organization_partition_to_be_processed=get_partition_info(source_name,'bronze_organizations','administration')
bronze_organization_predicate= " OR ".join(bronze_organization_partition_to_be_processed)
print(bronze_organization_predicate)

# COMMAND ----------

#Load data
bronze_organization_df=spark.sql(f"""
        select * from lakehouse_dev.administration.bronze_organizations
        where {bronze_organization_predicate}
        """)

display(bronze_organization_df)


# COMMAND ----------

#apply DQ checks

# python OpsEngine - python setup.py bdist_wheel - create wheel file and upload into s3 location 
# https://greatexpectations.io/expectations/expect_column_values_to_match_regex

dq_spec={
  "dq_specs": [
    {
      "dq_name": "silver-administration-organization_dq_checks",
      "input_id": "bronze_organization_df",
      "dq_type": "validator",
      "store_backend": "file_system",
      "local_fs_root_dir": "/Volumes/lakehouse_dev/administration/dq/silver/silver-administration-organization/root_dir",
      "data_docs_local_fs":"/Volumes/lakehouse_dev/administration/dq/silver/silver-administration-organization/data_docs",
      "result_sink_db_table": "lakehouse_dev.administration.administration_dq_results",
      "result_sink_location": "s3://lakehouse-administration1/silver/dq/administration/results",
      "fail_on_error": False,
      "result_sink_explode": True,
      "tag_source_data": True,
      "gx_result_format": "COMPLETE",
      "unexpected_rows_pk": [
        "Id"
      ],
      "dq_functions": [
        {
          "function": "expect_column_values_to_match_regex",
          "args": {
            "column": "PHONE",
             "regex":"^(\d{3}[- ]?)?\d{3}[- ]?\d{4}$"
          }
        },
        {
          "function": "expect_column_values_to_match_regex",
          "args": {
            "column": "ZIP",
             "regex":"^\d{5}$"

          }
        }


      ]
    }
  ]
}


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from mlops.dq_processors.dq_loader import DQLoader
dq_loader=DQLoader(dq_spec)
df_dict={"bronze_organization_df":bronze_organization_df}
dq_df_dict=dq_loader.process_dq(spark,df_dict)
dq_df=dq_df_dict["silver-administration-organization_dq_checks"]

# COMMAND ----------

display(dq_df)

# COMMAND ----------

#identify good records and bad records
good_record_df=dq_df.where("dq_validations.run_row_success==true").drop('dq_validations')
bad_records_df=dq_df.where("dq_validations.run_row_success==false")

# COMMAND ----------

display(bad_records_df)

# COMMAND ----------

from pyspark.sql.functions import day, month, col, current_timestamp, year,lit

bad_records_df= bad_records_df\
                              .withColumn("dq_audit_ingestion_timestamp", current_timestamp())\
                               .withColumn("Audit_Day", day(col("audit_ingestion_timestamp")))\
                               .withColumn("Audit_Month", month(col("audit_ingestion_timestamp")))\
                               .withColumn("Audit_Year", year(col("audit_ingestion_timestamp")))\
                                .withColumn("audit_table_name", lit("silver-administration-organization"))

partition_column=["Audit_Year","Audit_Month","Audit_Day","audit_table_name"]

bad_records_df.write.mode("append").partitionBy(partition_column).parquet("s3://lakehouse-administration1/dq/silver/silver-administration-organization/bad_records")
                                  

# COMMAND ----------

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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

db_bucket=config["bucket-name"]
prefix_name=config["silver-prefix"]
table_name="organizations"
catalog=config["catalog-name"]
schema_name=config["schema-name"]
partitions=["year","month","day"]
target_path=f"s3://{db_bucket}/{prefix_name}/{table_name}"
target_table_name="silver_"+table_name
target_table_path=f"{catalog}.{schema_name}.{target_table_name}"

good_record_df\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .partitionBy(*partitions)\
    .option("path",target_path)\
    .saveAsTable(target_table_path)

# COMMAND ----------

partitions_info=identify_partitions_predicate(good_record_df,partitions)
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

# COMMAND ----------

print(bronze_organization_partition_id_to_be_processed)

# COMMAND ----------

#Apply transformations to the good records

spark.sql(f"""
          update lakehouse_dev.administration.pipeline_audit_log_table
          set processed_status_info_array= array_union(processed_status_info_array,array('{source_name}'))
          where audit_id in ('{bronze_organization_partition_id_to_be_processed}')
          
            """

