# Databricks notebook source
# MAGIC %sql
# MAGIC select pais, sum(preco) as total_vendido from vinho
# MAGIC where preco > 0
# MAGIC group by pais
# MAGIC order by total_vendido desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pais, variante, SUM(preco) AS total_vendido
# MAGIC FROM vinho
# MAGIC WHERE preco > 0
# MAGIC GROUP BY pais, variante
# MAGIC ORDER BY total_vendido DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select pontos, preco from vinho