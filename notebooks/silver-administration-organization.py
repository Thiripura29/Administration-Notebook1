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


# COMMAND ----------

import json
from pyspark.sql.functions import *

#standartize the columns and do transalations
def standardized_columns_names(df):
    new_df_columns = [column.lower() for column in df.columns]
    return df.toDF(*new_df_columns)

def get_spark_schema_df(input_df):
    #schema validations and drifting
    spark_schema_json_tmp=json.loads(input_df.schema.json())
    print(spark_schema_json_tmp)
    spark_schema_json={"schema":[]}
    spark_schema=spark_schema_json["schema"]

    for field in spark_schema_json_tmp["fields"]:
        spark_schema.append(
            {
                "name":field["name"],
                "type":field["type"]
            }
                            )
        print(spark_schema)
    return sc.parallelize(spark_schema).toDF()

def get_user_defined_schema(user_defined_schema_json):
    new_col_schema=["format","how","is_partition","name","partition_level","partition_stratrgy","user_type"]
    return sc.parallelize(user_defined_schema_json["schema"]).toDF(new_col_schema)

def merge_spark_user_defined_schema(user_defined_schema_df,spark_schema_df):
    return user_defined_schema_df.join(spark_schema_df,on="name",how="left")

def get_schema_drift_df(input_df):
    tmp_df=input_df.select("name","type","user_type","format","how","is_partition","partition_level","partition_stratrgy")
    return tmp_df.filter(udf_compare_spark_type_userdefined_type(col("type"),col("user_type")))

def get_missing_value_df(schema_drift_df):
    default_values={"int":None,"long":None,"string":None,"struct":None,"list":None,"date":None,"boolean":None,"timestamp":None,"date":None}
    missing_columns_df=schema_drift_df.where("type IS NULL and is_partition != 'yes'")
    missing_columns=missing_columns_df.collect()
    return missing_columns

def get_assign_new_datatype_df(argument):
    switcher={
        "int":IntegerType(),
        "string":StringType(),
        "long":LongType(),
        "boolean":BooleanType(),
        "date":DateType(),
    }
    return switcher.get(argument,"datatype not found")

def get_assign_new_datatype_df(main_df,schema_drift_df):
    type_mismatch_df=schema_drift_df.where("type IS NOT NULL and is_partition != 'yes'")
    type_mismatch=type_mismatch_df.collect()
    for item in type_mismatch:
        columnname=item[0]
        current_datatype_string=item[2]
        user_data_type=item[1]
        print(f"user data type is : {user_data_type}")  
        if user_data_type !="timestamp":
            new_data_type=get_assign_new_datatype_df(user_data_type)
            if new_data_type !="datatype not found":
                main_df=main_df.withColumn(columnname,col(columnname).cast(new_data_type))
            else:
                print(f"data type: {user_data_type} not found for column: {columnname}")
                print("exited")
                exit(0)
        else:
            if current_datatype_string =="string":
                timestamp_format=item[3]
                main_df=main_df.withColumn(columnname,to_timestamp(col(columnname),timestamp_format))
        return main_df








# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import *

#UDF#
###################
@udf(returnType=BooleanType())
def udf_compare_spark_type_userdefined_type(sparktype,user_defined_type):
    return not (sparktype==user_defined_type)

# COMMAND ----------

standardized_columns_df=standardized_columns_names(bronze_organization_df)
main_df=standardized_columns_df
spark_schema_df=get_spark_schema_df(standardized_columns_df)
display(spark_schema_df)
user_defined_schema_df=get_user_defined_schema(schema)
display(user_defined_schema_df)
schema_joined_df=merge_spark_user_defined_schema(user_defined_schema_df,spark_schema_df)
display(schema_joined_df)
schema_drift_df=get_schema_drift_df(schema_joined_df)
display(schema_drift_df)
get_missing_columns=get_missing_value_df(schema_drift_df)
print(get_missing_columns)
# if len(get_missing_columns)>0:
#     raise Exception("user defined schema is not matched with spark source schema")
standardardize_df=get_assign_new_datatype_df(main_df,schema_drift_df)
display(standardardize_df)

# COMMAND ----------

schema= {
     "schema":[
        {"name":"id","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0},
        {"name":"NAME","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0},
        {"name":"ADDRESS","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0},
        {"name":"CITY","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0},   
        {"name":"STATE","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0},   
        {"name":"ZIP","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"LAT","type":"double","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"LON","type":"double","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"PHONE","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"REVENUE","type":"double","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"UTILIZATION","type":"string","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"audit_source_record_id","type":"long","format":"NA","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"audit_ingestion_timestamp","type":"timestamp","format":"yyyy-MM-dd'T'HH:mm:ss.SSSS","is_partition":"no","partition_stratrgy":"NA","how":"NA","partition_level":0}, 
        {"name":"date_partition","type":"date","format":"NA","is_partition":"yes","partition_stratrgy":"date","how":"to_date(current_timestamp)","partition_level":1}       
              ]
    }
