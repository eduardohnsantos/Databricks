# Databricks notebook source
# MAGIC %md
# MAGIC ### Criação de um dataframe com dados

# COMMAND ----------

#criando um dataframe com dados fixos
dados =[("Grimaldo ","Oliveira","Brasileira","Professor","M",3000),
("Ana ","Santos","Portuguesa","Atriz","F",4000),
("Roberto","Carlos","Brasileira","Analista","M",4000),
("Maria ","Santanna","Italiana","Dentista","F",6000),
("Jeane","Andrade","Portuguesa","Medica","F",7000)]
colunas=["Primeiro_Nome","Ultimo_nome","Nacionalidade","Trabalho","Genero","Salario"]
datafparquet=spark.createDataFrame(dados,colunas)
datafparquet.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravando o arquivo parquet

# COMMAND ----------

#criando o arquivo parquet
datafparquet.write.parquet("/FileStore/tables/parquet/pessoal.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subscrevendo o arquivo parquet

# COMMAND ----------

#Permite uma atualização do arquivo parquet
datafparquet.write.mode('overwrite').parquet('/FileStore/tables/parquet/pessoal.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo o arquivo parquet e guardando em um dataframe

# COMMAND ----------

#Realizando uma atualização do arquivo parquet
datafleitura= spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet")
datafleitura.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando uma consulta SQL

# COMMAND ----------

#Criando uma consulta em SQL
datafleitura.createOrReplaceTempView("Tabela_Parquet")
ResultSQL = spark.sql("select * from Tabela_Parquet where salario >= 6000 ")
ResultSQL.show()