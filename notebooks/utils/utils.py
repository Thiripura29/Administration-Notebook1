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
        with open(config_path, 'r') as f:
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



