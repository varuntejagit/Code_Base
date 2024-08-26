# Databricks notebook source
def read_data_from_file(spark, bucket_name, format,options,path):
    # Reads data from a specified S3 bucket and folder prefix, inferring schema and using the first row as headers
    return spark\
            .read\
            .format(format)\
            .options(**options)\
            .load(path)

# COMMAND ----------

def read_data_from_sql(spark, sql_query):
    return spark.sql(sql_query)

# COMMAND ----------

def write_data(df, bucket_name, database_name, table_name,write_mode, path, format):
    # Writes the DataFrame to a Parquet file in the specified S3 bucket and folder prefix
    # Parameters:
    # df: The DataFrame to write
    # bucket_name: The name of the S3 bucket where the data will be written
    # database_name: The name of the database to associate the table with
    # write_mode: The write mode (e.g., append, overwrite)
    # folder_prefix: The folder prefix within the bucket to write the data to
    
    # The data is written in Parquet format to the specified path and also saved as a table in the database
    return df\
            .write\
            .format(format)\
            .mode(write_mode)\
            .option("path", path)\
            .saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

def process_source(source_config):
    source_path = source_config["path"]
    source_type = source_config["type"]
    source_format = source_config["format"]
    source_spark_options = source_config.get("spark_options",{})
    source_df = spark.read.format(source_format).load(source_path,**source_spark_options)
    return source_df

# COMMAND ----------

def process_sink(sink_config,df):
    sink_path = sink_config["path"]
    sink_mode = sink_config["mode"]
    sink_database = sink_config.get("database")
    sink_table = sink_config.get("table")
    sink_format = sink_config["format"]
    sink_spark_options = sink_config.get("spark_options",{})
    if sink_database and sink_table:
        df.write.format(sink_format).mode(sink_mode).option("path", sink_path).saveAsTable(f"{sink_database}.{sink_table}")
    else:
        df.write.format(sink_format).mode(sink_mode).save(sink_path)

# COMMAND ----------

def process_source_sink(source_sink_config):
    source_config = source_sink_config["source"]
    sink_config = source_sink_config["sink"]
    df = process_source(source_config)
    process_sink(sink_config,df)
