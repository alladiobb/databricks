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

# MAGIC %python
# MAGIC #gravaçã dos dados que estão no dataframe para json em um único arquivo
# MAGIC df3.write.json("/FileStore/tables/JSON/eventos.json")

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("CREATE OR REPLACE TEMPORARY VIEW view_evento USING json OPTIONS" +
# MAGIC     " (path '/FileStore/tables/JSON/eventos.json')")
# MAGIC     spark.sql("select action from view_evento").show()
