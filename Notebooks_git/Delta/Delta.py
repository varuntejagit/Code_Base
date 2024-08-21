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
# MAGIC -- it will say history of operations done over a table or data 
# MAGIC describe history lakehouse_dev.default.test_table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- when we perform action like insert a new paruet file is created and if we 
# MAGIC insert into  lakehouse_dev.default.test_table values (1,"John",30,'New York','NY',100000.0)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.default.test_table

# COMMAND ----------

display(spark.read.parquet("s3://varun-databricks-s3-bucket/parquet_file_logs_view/part-00000-23606455-aa8f-4746-b018-6abb248456ee-c000.snappy.parquet"))
# it is used to read data in the paruet file created for that we need to paste the paruet file in the some other file in storage it is spark constrait 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  lakehouse_dev.default.test_table values (2,"Varun",24,'Allagadda','AP',500000.0)
