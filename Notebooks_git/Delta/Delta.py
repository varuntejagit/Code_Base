# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS lakehouse_dev.default.test_table(
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   age INT,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   salary FLOAT
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://varun-databricks-s3-bucket/test_table'

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC -- 
# MAGIC describe history lakehouse_dev.default.test_table
# MAGIC
# MAGIC
