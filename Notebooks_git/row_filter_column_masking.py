# Databricks notebook source
# MAGIC %md
# MAGIC - https://docs.databricks.com/en/views/dynamic.html
# MAGIC - https://docs.databricks.com/en/tables/row-and-column-filters.html

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP FUNCTION IF EXISTS lakehouse_dev.health_care.row_filter;
# MAGIC
# MAGIC CREATE FUNCTION lakehouse_dev.health_care.row_filter(type STRING)
# MAGIC RETURN IF(is_member('admins'), true, type='CHARGE');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakehouse_dev.health_care.delta_claims_transcations SET ROW FILTER lakehouse_dev.health_care.row_filter ON (type);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT type, count(*) FROM lakehouse_dev.health_care.delta_claims_transcations group by type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION lakehouse_dev.health_care.notes_mask(NOTES STRING)
# MAGIC   RETURN CASE WHEN is_member('admins') THEN NOTES  ELSE '******' END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakehouse_dev.health_care.delta_claims_transcations ALTER COLUMN notes SET MASK lakehouse_dev.health_care.notes_mask;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.health_care.delta_claims_transcations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alias the field 'email' to itself (as 'email') to prevent the
# MAGIC -- permission logic from showing up directly in the column name results.
# MAGIC CREATE OR REPLACE VIEW lakehouse_dev.health_care.delta_claims_transcations_view AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   CASE WHEN is_member('admins') THEN NOTES  ELSE hash(NOTES) END AS notes,
# MAGIC   claimid,
# MAGIC   chargeid,
# MAGIC   amount
# MAGIC FROM lakehouse_dev.health_care.delta_claims_transcations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.health_care.delta_claims_transcations_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse_dev.health_care.delta_claims_transcations_view
