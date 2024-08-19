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

def write_data(df, bucket_name, database_name, write_mode, path):
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
            .format("parquet")\
            .mode(write_mode)\
            .option("path", path)\
            .saveAsTable(f"{database_name}.{folder_prefix}")

# COMMAND ----------

def write_partitions_data(df, database_name, table_name,write_mode, partition_columns,path):
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
            .format("parquet")\
            .mode(write_mode)\
            .option("path", path)\
            .partitionBy(partition_columns)\
            .saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

# Define the list of folder prefixes to process
folder_prefixes = ["allergies", "claims_transcations", "claims", "paitents", "payers"]

# Specify the source and destination S3 bucket names
source_bucket_name = 'prudhvi-08052024-test'
destination_bucket_name = 'prudhvi-08052024-test'

# Define the catalog and schema names for the database
catalog_name = 'lakehouse_dev'
schema_name = 'health_care'

# Set the write mode for saving data
write_mode = "overwrite"

# Construct the full database name using the catalog and schema names
database_name = f"{catalog_name}.{schema_name}"

options = {
    "header": "true",
    "inferSchema": "true"
}

# COMMAND ----------

table_name = f"{catalog_name}.{schema_name}.claims_transcations"

# COMMAND ----------

read_path = f"s3://{source_bucket_name}/dataset/claims_transcations"
# Read data from the source bucket for the current folder prefix
df = read_data_from_file(spark, source_bucket_name, 'csv', options,read_path)
# Write the DataFrame to the destination bucket and register it as a table in the database
write_path = f"s3://{destination_bucket_name}/dataset/parition_by/parquet/claims_transcations/"

write_partitions_data(df, database_name, 'claims_transactions_partition' ,write_mode, ['PROCEDURECODE'],write_path)

# COMMAND ----------

# Loop through each folder prefix to process the data
for folder_prefix in folder_prefixes:
    read_path = f"s3://{source_bucket_name}/dataset/{folder_prefix}"
    print(read_path)
    # Read data from the source bucket for the current folder prefix
    df = read_data_from_file(spark, source_bucket_name, 'csv', options,read_path)
    # Write the DataFrame to the destination bucket and register it as a table in the database
    write_path = f"s3://{destination_bucket_name}/dataset/parquet/{folder_prefix}"
    write_data(df, destination_bucket_name, database_name, write_mode, write_path)

# COMMAND ----------

sql_query = f"""
    SELECT * FROM {database_name}.claims_transcations
"""

df = read_data_from_sql(spark, sql_query)
display(df)

# COMMAND ----------

sql_query = f"""
    SELECT * FROM lakehouse_dev.health_care.claims_transactions_partition where procedurecode = '10'
"""

df = read_data_from_sql(spark, sql_query)
display(df)
# df.write.format('parquet').save(f"s3://{source_bucket_name}/dataset/claims_transcations_dummy")

# COMMAND ----------

sql_query = f"""
    SELECT COUNT(*) AS total_transactions,
           MIN(fromdate) AS first_transaction_date,
           MAX(fromdate) AS last_transaction_date,
           SUM(amount) AS total_amount,
           AVG(amount) AS avg_amount
    FROM {database_name}.claims_transcations
"""

df = spark.sql(sql_query)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Charges by Patient and Type
# MAGIC This query sums the AMOUNT by PATIENTID and TYPE, giving you an overview of the total charges each patient has accumulated for different types of services.

# COMMAND ----------

table_name = f"{database_name}.claims_transcations"

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

sql_query = f"""
SELECT 
    PATIENTID, 
    TYPE, 
    SUM(AMOUNT) AS TOTAL_AMOUNT 
FROM 
    {table_name}
GROUP BY 
    PATIENTID, 
    TYPE
ORDER BY 
    TOTAL_AMOUNT DESC;
"""
display(spark.sql(sql_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Sum of Charges by Patient over Time
# MAGIC Using a window function to calculate the running total of charges (AMOUNT) for each patient over time (FROMDATE).

# COMMAND ----------

sql_query = f"""
SELECT 
    PATIENTID, 
    FROMDATE, 
    AMOUNT, 
    SUM(AMOUNT) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE) AS CUMULATIVE_AMOUNT
FROM 
    {table_name};
"""
display(spark.sql(sql_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ranking Patients by Total Charges
# MAGIC Rank patients based on their total charges, allowing you to see who the top spenders are.

# COMMAND ----------

sql_query = f"""
SELECT 
    PATIENTID, 
    SUM(AMOUNT) AS TOTAL_AMOUNT,
    RANK() OVER (ORDER BY SUM(AMOUNT) DESC) AS RANKING
FROM 
    {table_name}
GROUP BY
    PATIENTID
"""
display(spark.sql(sql_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Charge per Procedure Code by Place of Service
# MAGIC Find the average amount billed (AMOUNT) for each procedure code (PROCEDURECODE) in different places of service (PLACEOFSERVICE).

# COMMAND ----------

sql_query = f"""
SELECT 
    PLACEOFSERVICE, 
    PROCEDURECODE, 
    AVG(AMOUNT) AS AVG_AMOUNT
FROM 
    {table_name}
GROUP BY 
    PLACEOFSERVICE, 
    PROCEDURECODE;
"""
display(spark.sql(sql_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 3 Procedures by Total Units within Each Department
# MAGIC Use a window function to find the top 3 most frequently performed procedures in terms of units (UNITS) within each department (DEPARTMENTID).

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Complex Transformations
# MAGIC ### Identifying Outliers in Charges by Patient Using Z-Scores
# MAGIC Calculate the Z-score for each charge amount to identify outliers.
# MAGIC The Z-score, also known as the standard score, is a statistical measure that describes the position of a data point relative to the mean of a group of data points. It indicates how many standard deviations a data point is from the mean of the dataset.
# MAGIC
# MAGIC Z= X−μ/σ
# MAGIC
# MAGIC X: The value of the data point.
# MAGIC μ (mu): The mean of the dataset.
# MAGIC σ (sigma): The standard deviation of the dataset.
# MAGIC Interpretation:
# MAGIC Z-score = 0: The data point is exactly at the mean.
# MAGIC Z-score > 0: The data point is above the mean.
# MAGIC Z-score < 0: The data point is below the mean.
# MAGIC Z-score > 2 or < -2: The data point is generally considered to be an outlier or an extreme value, depending on the context. In many cases, a Z-score above 3 or below -3 is considered an outlier.
# MAGIC Use Cases:
# MAGIC Outlier Detection: Z-scores can help identify outliers in the data, as extreme Z-scores indicate data points that are far from the mean.
# MAGIC Normalization: Z-scores are used to normalize data, making different datasets comparable by converting them to a common scale with a mean of 0 and a standard deviation of 1.
# MAGIC Hypothesis Testing: Z-scores are used in various statistical tests to determine how unusual or typical a certain observation is within a dataset.
# MAGIC Example:
# MAGIC Suppose the mean height of a group of people is 170 cm, with a standard deviation of 10 cm. If a person is 180 cm tall, their Z-score would be:
# MAGIC
# MAGIC Z= 180−170/10 = 1
# MAGIC
# MAGIC This Z-score of 1 indicates that the person's height is 1 standard deviation above the mean.

# COMMAND ----------

sql_query = f"""
SELECT * FROM (
    SELECT 
        PATIENTID, 
        CLAIMID, 
        CHARGEID, 
        AMOUNT, 
        AVG(AMOUNT) OVER (PARTITION BY PATIENTID) AS AVG_AMOUNT,
        STDDEV(AMOUNT) OVER (PARTITION BY PATIENTID) AS STD_DEV_AMOUNT,
        (AMOUNT - AVG(AMOUNT) OVER (PARTITION BY PATIENTID)) / 
        NULLIF(STDDEV(AMOUNT) OVER (PARTITION BY PATIENTID), 0) AS Z_SCORE
    FROM 
        {table_name}
) AS subquery
WHERE 
    ABS(Z_SCORE) > 3;
"""

display(spark.sql(sql_query))

# COMMAND ----------

# MAGIC %md
# MAGIC Breakdown of the Query:
# MAGIC 1. PARTITION BY PATIENTID:
# MAGIC
# MAGIC > The query groups the data by PATIENTID, meaning that the calculations (average, standard deviation) are performed within each patient's group of charges.
# MAGIC
# MAGIC 2. AVG(AMOUNT) OVER (PARTITION BY PATIENTID) AS AVG_AMOUNT:
# MAGIC
# MAGIC > This calculates the average (AVG) of the AMOUNT for each patient and assigns it to AVG_AMOUNT.
# MAGIC
# MAGIC 3. STDDEV(AMOUNT) OVER (PARTITION BY PATIENTID) AS STD_DEV_AMOUNT:
# MAGIC
# MAGIC > This calculates the standard deviation (STDDEV) of the AMOUNT for each patient and assigns it to STD_DEV_AMOUNT.
# MAGIC
# MAGIC 4. Z-Score Calculation:
# MAGIC
# MAGIC > The Z-score is calculated as the difference between the charge amount (AMOUNT) and the average amount (AVG_AMOUNT), divided by the standard deviation (STD_DEV_AMOUNT).
# MAGIC The NULLIF function is used to prevent division by zero by returning NULL if the standard deviation is zero.
# MAGIC
# MAGIC 5. Where Clause:
# MAGIC
# MAGIC > The HAVING clause filters out records where the absolute value of the Z-score is not greater than 3, effectively identifying the outliers.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import avg, stddev, col, abs, when

# Define a window specification partitioned by PATIENTID
window_spec = Window.partitionBy("PATIENTID")

sql_query = f"""
    SELECT * FROM {database_name}.claims_transcations
"""

df = read_data_from_sql(spark, sql_query)

# Calculate the average, standard deviation, and Z-score
df_with_stats = df.withColumn("AVG_AMOUNT", avg("AMOUNT").over(window_spec)) \
                  .withColumn("STD_DEV_AMOUNT", stddev("AMOUNT").over(window_spec)) \
                  .withColumn("Z_SCORE", 
                              (col("AMOUNT") - col("AVG_AMOUNT")) / 
                              when(col("STD_DEV_AMOUNT") != 0, col("STD_DEV_AMOUNT")).otherwise(None))

# Filter the DataFrame to get only the records where the absolute value of Z-score is greater than 3
outliers = df_with_stats.filter(abs(col("Z_SCORE")) > 3)

# Select the relevant columns
outliers.select("PATIENTID", "CLAIMID", "CHARGEID", "AMOUNT", "AVG_AMOUNT", "STD_DEV_AMOUNT", "Z_SCORE").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding Seasonal Trends in Charges
# MAGIC Analyzing charges (AMOUNT) over time to detect seasonal patterns by comparing charges in the same month across different years.
# MAGIC

# COMMAND ----------

sql_query = f"""
-- Step 1: Calculating Monthly Averages (Monthly_Averages CTE)
-- This part of the query calculates the average AMOUNT for each patient on a monthly basis.
WITH Monthly_Averages AS (
    SELECT 
        PATIENTID, 
        YEAR(FROMDATE) AS YEAR, 
        MONTH(FROMDATE) AS MONTH, 
        AVG(AMOUNT) AS AVG_MONTHLY_AMOUNT
    FROM 
        {table_name}
    GROUP BY 
        PATIENTID, 
        YEAR(FROMDATE), 
        MONTH(FROMDATE)
),
-- Applying the LAG Function (Lagged_Amounts CTE)
-- This CTE computes the PREVIOUS_YEAR_AMOUNT for each patient by using the LAG function.
Lagged_Amounts AS (
    SELECT 
        PATIENTID, 
        YEAR, 
        MONTH, 
        AVG_MONTHLY_AMOUNT,
        --  Retrieves the AVG_MONTHLY_AMOUNT from the same month of the previous year for each patient.
        --  The data is partitioned by PATIENTID and MONTH, ensuring that the LAG function only considers the previous year's data within the same month for each patient.
        --  The data is ordered by YEAR within each partition to ensure the correct calculation of the lagged value.
        LAG(AVG_MONTHLY_AMOUNT) OVER (PARTITION BY PATIENTID, MONTH ORDER BY YEAR) AS PREVIOUS_YEAR_AMOUNT
    FROM 
        Monthly_Averages
)
-- 3. Calculating the Change and Filtering the Results
SELECT 
    PATIENTID, 
    YEAR, 
    MONTH, 
    AVG_MONTHLY_AMOUNT,
    -- This calculates the difference between the current year's average monthly amount and the previous year's corresponding month.
    (AVG_MONTHLY_AMOUNT - PREVIOUS_YEAR_AMOUNT) AS AMOUNT_CHANGE
FROM 
    Lagged_Amounts
WHERE 
    --  This condition ensures that only records with a valid PREVIOUS_YEAR_AMOUNT are included in the final result, filtering out any rows where the lagged value is NULL
    PREVIOUS_YEAR_AMOUNT IS NOT NULL
ORDER BY 
    PATIENTID, 
    MONTH, 
    YEAR;
"""

df = read_data_from_sql(spark, sql_query)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lag and Lead Analysis of Patient Charges
# MAGIC analyze the sequence of healthcare claims for each patient by calculating the time difference between consecutive claims. Here's a detailed explanation of each part of the query
# MAGIC
# MAGIC Window Functions:
# MAGIC > LAG(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE) AS PREVIOUS_CHARGE_DATE:
# MAGIC
# MAGIC   This function retrieves the FROMDATE of the previous claim made by the same patient.
# MAGIC   PARTITION BY PATIENTID: Groups the data by PATIENTID, so that the function operates within each patient's claims history.
# MAGIC   ORDER BY FROMDATE: Orders the claims by date to ensure that the previous claim is correctly identified.
# MAGIC   PREVIOUS_CHARGE_DATE: Alias for the date of the previous claim.
# MAGIC
# MAGIC > LEAD(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE) AS NEXT_CHARGE_DATE:
# MAGIC
# MAGIC   This function retrieves the FROMDATE of the next claim made by the same patient.
# MAGIC   The rest of the logic is the same as the LAG function, but it looks forward instead of backward.
# MAGIC   NEXT_CHARGE_DATE: Alias for the date of the next claim.
# MAGIC
# MAGIC Date Difference Calculations:
# MAGIC > DATEDIFF(FROMDATE, LAG(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE)) AS DAYS_SINCE_LAST_CHARGE:
# MAGIC
# MAGIC   This function calculates the number of days between the current claim (FROMDATE) and the previous claim (LAG(FROMDATE)).
# MAGIC   DAYS_SINCE_LAST_CHARGE: Alias for the number of days since the last claim.
# MAGIC > DATEDIFF(LEAD(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE), FROMDATE) AS DAYS_UNTIL_NEXT_CHARGE:
# MAGIC
# MAGIC   This function calculates the number of days between the current claim (FROMDATE) and the next claim (LEAD(FROMDATE)).
# MAGIC   DAYS_UNTIL_NEXT_CHARGE: Alias for the number of days until the next claim.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     PATIENTID, 
# MAGIC     CLAIMID, 
# MAGIC     FROMDATE, 
# MAGIC     AMOUNT,
# MAGIC     LAG(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE) AS PREVIOUS_CHARGE_DATE,
# MAGIC     LEAD(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE) AS NEXT_CHARGE_DATE,
# MAGIC     DATEDIFF(FROMDATE, LAG(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE)) AS DAYS_SINCE_LAST_CHARGE,
# MAGIC     DATEDIFF(LEAD(FROMDATE) OVER (PARTITION BY PATIENTID ORDER BY FROMDATE), FROMDATE) AS DAYS_UNTIL_NEXT_CHARGE
# MAGIC FROM 
# MAGIC     lakehouse_dev.health_care.claims_transcations;
# MAGIC

# COMMAND ----------

"""
Identifying Top N Patients by Charge Growth Rate
Determine the top N patients who have shown the highest growth rate in charges over a specified period.
"""
from pyspark.sql.functions import sum, col, year

sql_query = f"""
    SELECT * FROM {database_name}.claims_transcations
"""

df = read_data_from_sql(spark, sql_query)

patient_growth = df.withColumn("YEAR", year("FROMDATE")) \
                   .groupBy("PATIENTID") \
                   .pivot("YEAR", [2022, 2023]) \
                   .agg(sum("AMOUNT").alias("AMOUNT")) \
                   .withColumn("GROWTH_RATE", (col("2023") - col("2022")) / col("2022"))

top_n_patients = patient_growth.orderBy(col("GROWTH_RATE").desc()).limit(10)

display(top_n_patients)
