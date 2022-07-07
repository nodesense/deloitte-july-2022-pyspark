# Databricks notebook source
# Spark has database
# but comman, spark is used with hive meta server or Hive Database catalog

# Hive catalog shall have database name, tables , schemas for the table, column, data type, then location where data is stored
# Hive catalog is only DDL - Data Definition Language [create/drop database, create/drop/alter tables... create temp view etc...]

# DML - Data Manipulation Language / insert/update/select/delete etc applied
# Legacy apps uses Hive Query Language (HQL) is used as Query Engine which will run the DML, uses Map Reduce from Hadoop - discontinued or obsoluted..
# Spark SQL - is a Query Engine which can use Hive Catalog for meta data [database, tables, columns, name, types etc] and Query teh data located in S3/DBFS/Google FS/ADLS/HADOOP/directory etc

# Data is stored in data lake like hdfs/s3 etc

# COMMAND ----------

# Databricks notebook source
spark # spark session based on hive

# COMMAND ----------

# dbutils.fs.rm("/user/hive/warehouse", True)

# COMMAND ----------

# create database called moviedb, DDL
# now check data menu on databricks left side
spark.sql ("CREATE DATABASE IF NOT EXISTS moviedb")

# COMMAND ----------

# List database, default is default db, moviedb we just created
spark.sql ("SHOW DATABASES").show()

# COMMAND ----------

# TWO TYPES OF TABLES in Hive
# 1. Managed table - INSERT, UPDATE, DELETE to be performed by Spark Engine
#                    Data shall be stored in /user/hive/warehouse/database/table.db 
#                - Schema and data both managed by spark/hive meta data
# 2. Unmanaged table /external table
#        - schema is managed by spark/hive meta data
#        - data is managed by externally by people, or ETL tools add new files, remove exisiting file etc


# COMMAND ----------


# Create a managed table
# checl data menu, under moviedb database tables
# check dbfs .. warehouse dir, moviedb.db for table data
spark.sql("""
CREATE TABLE IF NOT EXISTS moviedb.reviews (movieId INT, review STRING)
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- spark sql
# MAGIC -- any sql here is executed by spark.sql and result is displayed using display fucntion, html output
# MAGIC 
# MAGIC  INSERT INTO moviedb.reviews VALUES (1, 'nice movie')

# COMMAND ----------

# MAGIC %sql
# MAGIC  INSERT INTO moviedb.reviews VALUES (2, 'funny')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

