# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/structured-streaming/events/file-1.json

# COMMAND ----------

# MAGIC %python
# MAGIC #lendo 1 arquivo JSON
# MAGIC df = spark.read.json("/databricks-datasets/structured-streaming/events/file-1.json")
# MAGIC df.printSchema()
# MAGIC df.show()

# COMMAND ----------

# MAGIC %python
# MAGIC #lendo dois arquivos JSON
# MAGIC df2.spark.read.json(["/databricks-datasets/structured-streaming/events/file-1.json","/databricks-datasets/structured-streaming/events/file-2.json"])
# MAGIC df2.show()

# COMMAND ----------

# MAGIC %python
# MAGIC #lendo dois ou mais arquivos JSON
# MAGIC df3.spark.read.json("/databricks-datasets/structured-streaming/events/*.json")
# MAGIC df3.show()

# COMMAND ----------


