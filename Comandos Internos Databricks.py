# Databricks notebook source
# MAGIC %md
# MAGIC ### Acessando o help de comando
# MAGIC  

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

# MAGIC %md
# MAGIC ### #Listando as pastas

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %md
# MAGIC ### #Listando as pastas

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando as pastas (exemplo, criando a pasta vendas)
# MAGIC ### 

# COMMAND ----------

# MAGIC %fs mkdirs vendas

# COMMAND ----------

# MAGIC %md
# MAGIC ### #Mostrando a pasta criada (exemplo, a pasta vendas)
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copiando um arquivo de uma pasta para outra

# COMMAND ----------

# MAGIC %fs cp /FileStore/tables/carga/vinhos_no_mundo.csv /FileStore/vendas2/copia_vinhos.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### #Copiando um arquivo de uma pasta para outra

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/carga/vinhos_no_mundo.csv", "/FileStore/vendas3/copia2_vinhos.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renomeando (troca de nome) de um arquivo

# COMMAND ----------

# MAGIC %fs mv /FileStore/vendas2/copia_vinhos.csv /FileStore/vendas2/copia_muda_vinhos.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renomeando (troca de nome) de um arquivo

# COMMAND ----------

dbutils.fs.mv("/FileStore/vendas2/copia_muda_vinhos.csv", "/FileStore/vendas2/copia_troca_vinhos.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Elimina um arquivo

# COMMAND ----------

# MAGIC %fs rm /FileStore/vendas2/copia_troca_vinhos.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Elimina um arquivo

# COMMAND ----------

# MAGIC %fs rm /FileStore/vendas3/copia2_vinhos.csv
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Elimina um arquivo dbutils.fs

# COMMAND ----------

dbutils.fs.rm("/FileStore/vendas3/copia2_vinhos.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Elimina a pasta

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/vendas2/

# COMMAND ----------

dbutils.fs.rm("/FileStore/vendas2/", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando uma pesquisa em bash-linux

# COMMAND ----------

# MAGIC %%bash
# MAGIC find /databricks -name "*.csv" | grep "fa"