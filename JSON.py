# Databricks notebook source
# MAGIC %md
# MAGIC ### Lista de arquivos Json que estão armazenados no DBFS

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exibindo um arquivo Json com as informações

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/structured-streaming/events/file-1.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Carregando 1 arquivo Json para o dataframe

# COMMAND ----------

# Lendo 1 arquivo JSON
dataf = spark.read.json("/databricks-datasets/structured-streaming/events/file-1.json")
dataf.printSchema()
dataf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregando 2 arquivos Json para o dataframe

# COMMAND ----------

#Lendo 2 arquivos JSON
dataf2 = spark.read.json(['/databricks-datasets/structured-streaming/events/file-1.json','/databricks-datasets/structured-streaming/events/file-2.json'])
dataf2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregando TODOS os arquivos Json para o dataframe

# COMMAND ----------

#Lendo todos os arquivos JSON
dataf3 = spark.read.json("/databricks-datasets/structured-streaming/events/*.json")
dataf3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unificando todos os arquivos que foram guardados no dataframe dataf3 para um novo arquivo JSON

# COMMAND ----------

#Gravação dos dados que estão no dataframe para JSON em um único arquivo
dataf3.write.json("/FileStore/tables/JSON/eventos.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação de uma tabela para executar SQL

# COMMAND ----------

spark.sql("CREATE OR REPLACE TEMPORARY VIEW view_evento USING json OPTIONS" +
" (path '/FileStore/tables/JSON/eventos.json')")
spark.sql("select action from view_evento").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from view_evento