# Databricks notebook source
# MAGIC %sql
# MAGIC alter table lakehouse_dev.administration.pipeline_audit_log_table
# MAGIC add columns (processed_status_info_array array<string>);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from lakehouse_dev.administration.pipeline_audit_log_table where audit_id in('f33f7f99-f1b9-4788-8d89-c337f601baaf')

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
    return(partition_to_be_processed,partition_id_to_be_processed)

# COMMAND ----------

source_name="silver-administration-organization"

# COMMAND ----------

bronze_organization_partition_to_be_processed,bronze_organization_partition_id_to_be_processed=get_partition_info(source_name,'bronze_organizations','administration')
bronze_organization_predicate=" OR ".join(bronze_organization_partition_to_be_processed)
print(bronze_organization_predicate)

# COMMAND ----------

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
      "result_sink_db_table": "lakehouse_dev.administration.administration_dq_spec",
      "result_sink_location": "/Volumes/lakehouse_dev/administration/dq/administration/results",
      "fail_on_error": False,
      "result_sink_explode": False,
      "tag_source_data": False,
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
