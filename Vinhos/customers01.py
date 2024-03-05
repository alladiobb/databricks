# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_bronze;
# MAGIC COPY INTO customers_bronze
# MAGIC     FROM 'dbfs:/mnt/dbacademy-datasets/get-started-with-data-engineering-on-databricks/v01/'
# MAGIC     FILEFORMAT = CSV
# MAGIC     FORMAT_OPTIONS ('inferSchema' = 'true', 'header' = 'true')
# MAGIC     COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXIST customers_silver
# MAGIC   DEEP CLONE customers_bronze
# MAGIC
# MAGIC   SELECT * FROM customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver_merged
# MAGIC (customer_id int,
# MAGIC tax_id double,
# MAGIC tax_code string,
# MAGIC customer_name string,
# MAGIC state string,
# MAGIC city string,
# MAGIC postcode string,
# MAGIC street string);
# MAGIC MERGE INTO customers_silver_merged
# MAGIC   USING customer_bronze
# MAGIC   ON customers_bronze.customer_id = customers_silver_merged.customer_id
# MAGIC   WHEN NOT MATCHED THEN INSERT *;
# MAGIC
