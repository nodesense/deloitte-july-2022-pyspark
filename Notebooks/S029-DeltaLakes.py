# Databricks notebook source
# Databricks notebook source
# delta logs _delta_logs created for transaction purpose, 
# _delta_logs directory shall not contain data instead some meta about operations performed on delta lakes
# _detals_logs shall have schema, entries of any files are added or removed

# using delta lakes, we can perform insert/update/delete operations, spark uses _delta_logs directory for tallying added/removed files
# Databricks notebook source
# to help to print data in delta_logs files..since we cannot preview json easily in databricks platform

def showJson(path):
  import json
  contents = sc.textFile(path).collect()
  for content in contents:
    result = json.loads(content)
    print(json.dumps(result, indent = 3))

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse/branddb.db", True)
spark.sql("DROP DATABASE IF EXISTS branddb")

# COMMAND ----------


spark.sql ("CREATE DATABASE branddb")

# COMMAND ----------

# Delta lake shall have meta data about table, columns, data types etc who created 
spark.sql ("CREATE TABLE branddb.brands (id INT, name STRING)")

# COMMAND ----------



showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000000.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- this will create a parquet file inside brands directory, 
# MAGIC -- the new parquet file shall be added into new json file, placed under delta logs 0000000xxx1.json
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO branddb.brands VALUES(1, 'Apple')

# COMMAND ----------


showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000001.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- this will create a parquet file inside brands directory
# MAGIC -- the new parquet file shall be added into new json file, placed under delta logs 0000000xxx1.json
# MAGIC INSERT INTO branddb.brands VALUES(2, 'Google')

# COMMAND ----------

showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000002.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- spark queries uses json files locatd in _delta_logs folder/directory
# MAGIC -- spark tally add/remove parquet fiels in json
# MAGIC -- then query the data from applicable files [not in removed files]
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update
# MAGIC -- delta log, the existing file that contains Google record shall be removed, not physically but in the _delta_logs entries
# MAGIC -- however on the data/table directory, the file shall be existing even though it was marked for removal, but removed files are not taken for query purpose
# MAGIC -- new file with updated value Alphabet shall be added in to data directory and also _delta_logs
# MAGIC UPDATE branddb.brands set name='Alphabet' where id=2

# COMMAND ----------

# 000000..03.json shall a file removed which contains google, shall have new file added, that contains Alphabet data
# when we run sql query, removed files even though they exist in the physical folder, it would be taken for query
showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000003.json")


# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- this query shall fetch parquet files added for apple, alphabet, will not pick google record as it was marked as deleted in _delta_logs
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the parquet which contains apple record shall be marked for deletion in _delta_logs
# MAGIC DELETE FROM branddb.brands where id = 1

# COMMAND ----------

showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000004.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC  -- time travel, Rollback to previous version, developer realized that removable of appled id 1 by mistake
# MAGIC 
# MAGIC INSERT INTO branddb.brands SELECT * from branddb.brands VERSION AS OF 2 WHERE id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now apple record recovered from deleted state
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- vaccume the deleted files
# MAGIC -- right it won't remove files, as minimum duration for retention is 7 days
# MAGIC -- any files older than 7 days removed
# MAGIC VACUUM branddb.brands 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- remove files older than 30 days
# MAGIC VACUUM branddb.brands RETAIN 720 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in genral we can't reduce tiem less than  7 days 
# MAGIC # error if we do less than 7 days or 168 hours
# MAGIC VACUUM branddb.brands RETAIN 48 HOURS

# COMMAND ----------

