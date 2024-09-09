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
# MAGIC -- it will say history of operations done over a table or data and version is like when we perform any action a new version will be created 
# MAGIC describe history lakehouse_dev.default.test_table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- if we want to see the data of particular version we can use this command 
# MAGIC select * from lakehouse_dev.default.test_table version as of 11

# COMMAND ----------

# MAGIC %sql
# MAGIC -- when we perform action like insert a new paruet file is created 
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
# MAGIC insert into  lakehouse_dev.default.test_table values (2,"Varun",24,'Allagadda','AP',500000.0);
# MAGIC insert into  lakehouse_dev.default.test_table values (3,"Teja",22,'Nandyal','AP',700000.0)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- when we perform UPDATE operatation on a table 
# MAGIC --suppose if we update a row and table has 3 rows delta will copy two old rows from original table along with the updated row and it will create a new paruet file.
# MAGIC --and old file will be deleted in the form of delta but physically still the file is still there in the storage untill we run vaccum command.
# MAGIC update lakehouse_dev.default.test_table set age =23 where id =1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- when we perform delete operation on a table which contains 3 rows we have new concept similar in insert here in delete operation delete vector index will be there if we delete id of 1 it will copy index of 1 in deletion vector and while printing table it will ignore the data which present in deletion vector 
# MAGIC delete from  lakehouse_dev.default.test_table where id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC create table  lakehouse_dev.default.new_test_table as select * from lakehouse_dev.default.test_table

# COMMAND ----------

# MAGIC
# MAGIC %run /Workspace/Repos/varunteja00098@gmail.com/Code_Base/Notebooks_git/common_utils

# COMMAND ----------

source_sinks = [
    {
        "source":{
            "path": "s3://varun-databricks-s3-bucket/dataset/parquet/allergies",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://varun-databricks-s3-bucket/Delta/allergies",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_allergies",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://varun-databricks-s3-bucket/dataset/parquet/claims_transcations",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://varun-databricks-s3-bucket/Delta/claims_transcations",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_claims_transcations",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://varun-databricks-s3-bucket/dataset/parquet/claims",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://varun-databricks-s3-bucket/Delta/claims",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_claims",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://varun-databricks-s3-bucket/dataset/parquet/paitents",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://varun-databricks-s3-bucket/Delta/paitents",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_paitents",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://varun-databricks-s3-bucket/dataset/parquet/payers",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://varun-databricks-s3-bucket/Delta/payers",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_payers",
            "format": "delta"
        }
    }
]

# COMMAND ----------

for source_sink in source_sinks:
    process_source_sink(source_sink)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   lakehouse_dev.health_care.delta_paitents dp
# MAGIC INNER JOIN 
# MAGIC   lakehouse_dev.health_care.delta_claims_transcations dct
# MAGIC ON dp.id = dct.PATIENTID
# MAGIC INNER JOIN
# MAGIC   lakehouse_dev.health_care.delta_allergies da
# MAGIC ON dp.id = da.PATIENT
