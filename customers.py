
# Databricks notebook source
# MAGIC %md
# MAGIC ### This is an example
# MAGIC This notebook creates a dataframe with some sample data that can be used for a quick visualization.

# COMMAND ----------
%sql
CREATE TABLE IF NOT EXISTS customers_bronze;
COPY INTO customers_bronze
    FROM 'dbfs:/mnt/dbacademy-datasets/get-started-with-data-engineering-on-databricks/v01/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('inferSchema' = 'true', 'header' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
# COMMAND ----------
