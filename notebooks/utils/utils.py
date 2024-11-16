# Databricks notebook source
def get_jdbc_credentials_from_scope(scope_name):
    try:
        user_name=dbutils.secrets.get(scope=scope_name,key="db_username")
        password=dbutils.secrets.get(scope=scope_name,key="db_password")
        return {"user_name": user_name, "password": password}
    except Exception as e:
        raise e

# COMMAND ----------

import traceback
import json
def load_config(config_path):
    """
    Load configuration from a JSON file.

    This method reads the JSON file specified by the `config_path` and
    returns the parsed configuration as a dictionary.

    :return: Parsed JSON configuration as a dictionary.
    :raises ValueError: If the config file format is not JSON.
    """
    try:
        with open(config_path,'r') as f:
            if config_path.endswith('.json'):
                return json.load(f)
            else:
                    raise ValueError("Unsupported config file format. Use JSON.")
    except Exception as e:
        print(traceback.format_exc())

# COMMAND ----------

def identify_partitions_predicate(df,partition_columns):

    conditions = []
    for row in df.select(*partition_columns).distinct().collect():
        conditions_parts=[f"{col_name}='{row[col_name]}'" for col_name in partition_columns]
        condition="AND ".join(conditions_parts)
        conditions.append(f"({condition})")

        #combine conditions with OR
        partition_predicate="OR ".join(conditions)

        return partition_predicate


# COMMAND ----------

from pyspark.sql import functions as F
import uuid
def make_audit_entry(audit_entry_json,audit_table_name):
    audit_entry_json["audit_id"]=str(uuid.uuid4())
    df=spark.createDataFrame([audit_entry_json])
    df=df.withColumn('created_date',F.current_timestamp())
    df.write.mode("append").saveAsTable(audit_table_name)




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

import json
from pyspark.sql.functions import *

#standartize the columns and do transalations
def standardized_columns_names(df):
    new_df_columns=[]
    for column in df.columns:
        new_df_columns.append(column)
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

def get_new_datatype_df(argument):
    switcher={
        "int":IntegerType(),
        "string":StringType(),
        "long":LongType(),
        "boolean":BooleanType(),
        "date":DateType(),
        "double":DoubleType()
    }
    return switcher.get(argument,"datatype not found")

def get_assign_new_datatype_df(main_df,schema_drift_df):
    type_mismatch_df=schema_drift_df.where("type IS NOT NULL and is_partition != 'yes'")
    type_mismatch=type_mismatch_df.collect()
    for item in type_mismatch:
        column_name=item[0]
        current_data_type_string=item[1]
        user_data_type=item[2]
        print(f"user data type is : {user_data_type}")  
        if user_data_type !="timestamp":
            new_data_type=get_new_datatype_df(user_data_type)
            if new_data_type !="datatype not found":
                main_df=main_df.withColumn(column_name,col(column_name).cast(new_data_type))
            else:
                print(f"data type: {user_data_type} not found for column: {column_name}")
                print("exited")
                exit(0)
        else:
            if current_datatype_string =="string":
                timestamp_format=item[3]
                main_df=main_df.withColumn(column_name,to_timestamp(col(column_name),timestamp_format))
    return main_df




# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import *

#UDF#
###################
@udf(returnType=BooleanType())
def udf_compare_spark_type_userdefined_type(sparktype,user_defined_type):
    return not (sparktype==user_defined_type)
