# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists lakehouse_dev.administration.pipeline_audit_log_table;
# MAGIC create external table lakehouse_dev.administration.pipeline_audit_log_table
# MAGIC (
# MAGIC audit_id string not null,
# MAGIC sink_name string not null,
# MAGIC data_load_trype string not null,
# MAGIC db_schema_name string not null,
# MAGIC db_table_name string not null,
# MAGIC data_storage_path string not null,
# MAGIC timestamp_or_id_column_name string not null,
# MAGIC last_processed_timestamp_or_id_column_value string,
# MAGIC partition_column_info string,
# MAGIC processed_status_info string,
# MAGIC created_date timestamp
# MAGIC ) 
# MAGIC using delta
# MAGIC location 's3://lakehouse-administration1/audit/pipeline_audit_log'
