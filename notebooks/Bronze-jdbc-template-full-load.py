# Databricks notebook source
# MAGIC %run ./utils/util

# COMMAND ----------

dbutils.widgets.text("config_path","/Workspace/Users/chinnasamythiripura@gmail.com/Administration-Notebook1/configs")

# COMMAND ----------

import json
config_path=dbutils.widgets.get("config_path")
config=load_config(config_path)


# COMMAND ----------

print(config_path)

# COMMAND ----------

import json
import traceback

config_path=dbutils.widgets.get("config_path")
config_data = None
with open(config_path, 'r') as f:
    if config_path.endswith('.json'):
        config_data= json.load(f)
    else:
        raise ValueError("Unsupported config file format. Use JSON.")
    display(config_data)

# COMMAND ----------



# COMMAND ----------

database_config=config["database-config"]
health_config=config["health-config"]


# COMMAND ----------

url=health_config["url"]
jdbc_credentials=get_jdbc_credentials_from_scope(health_config["scope_name"])
username=jdbc_credentials["username"]
password=jdbc_credentials["password"]
driver_name=health_config["driver_name"]

# COMMAND ----------

sources=health_config["sources"]
for source in sources:
    if source["load_type"]=="full_load":
spark.read.format("jdbc").options(
    {
        "url": url,
        "dbtable": "health_data"
        "user": username
        "password": password
    }
)
