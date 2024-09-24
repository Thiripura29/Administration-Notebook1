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
