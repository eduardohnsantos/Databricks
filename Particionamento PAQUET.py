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
# MAGIC ### Particionando os dados do arquivo parquet em grupos

# COMMAND ----------

#Particionando os dados em um arquivo parquet
datafparquet.write.partitionBy("Nacionalidade","salario").mode("overwrite").parquet("/FileStore/tables/parquet/pessoal.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Exibindo os dados do parquet cuja a nacionalidade é brasileira

# COMMAND ----------

#Lendo o aquivo participonado do parquet
datafnacional=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira")
datafnacional.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando uma pesquisa via SQL no arquivo parquet particionando

# COMMAND ----------

#Consultando diretamente o arquivo parquet particionado via SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW Cidadao USING parquet OPTIONS (path \"/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira\")")
spark.sql("SELECT * FROM Cidadao where Ultimo_nome='Oliveira'").show()