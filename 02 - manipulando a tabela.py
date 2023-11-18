# Databricks notebook source
# MAGIC %sql
# MAGIC select * from vinho limit 2

# COMMAND ----------

clientes = spark.read.format('csv').options(header='true',inferSchema='true',delimiter=';').load('/FileStore/tables/1_3_clientes_cartao.csv')
display(clientes)

# COMMAND ----------

# MAGIC %scala
# MAGIC val cliente = spark.read.format("csv")
# MAGIC .option("header", "true")
# MAGIC .option("inferSchema", "true")
# MAGIC .option("delimiter", ";")
# MAGIC .load("/FileStore/tables/1_3_clientes_cartao.csv")
# MAGIC cliente.createOrReplaceTempView("dados_cliente")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dados_cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC select pais, sum(preco) as total_vendido from vinho
# MAGIC where preco > 0
# MAGIC group by pais
# MAGIC order by total_vendido desc
# MAGIC limit 10

# COMMAND ----------


