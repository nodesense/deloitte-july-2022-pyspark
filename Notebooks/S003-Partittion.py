# Databricks notebook source
data = range(1, 20) # 1...10
rdd = sc.parallelize(data) # by default takes partittion size from tasks local[8]

# sc.<<TAB>><<TAB>><<TAB>> to get intelligence to show matching function names/attributes

# COMMAND ----------

# get number of partititons LAZY

rdd.getNumPartitions()

# COMMAND ----------

# collect - collect data from all partitions
rdd.collect()

# COMMAND ----------

# take function
# takes data from first partitions [first n records]
# if the first partittion does't have enough records, it picks data from second partitiion along with first one
rdd.take(7)

# COMMAND ----------

# glom - collect data from each partition, returns as list of list
rdd.glom().collect() # 1,2,3...7 are stored in separate partitions, 8 & 9 stored in same partitions

# COMMAND ----------

data = range(1, 10) # 1...9
# parallize takes default partitions from default parallism
print ("default parallism", sc.defaultParallelism)
print ("default defaultMinPartitions ", sc.defaultMinPartitions) # used while reading from hadoop as default

rdd1 = sc.parallelize(data, 2) # now load data into 2 partitions


# COMMAND ----------

rdd1.glom().collect()


# COMMAND ----------

data = range(1, 30)
rdd = sc.parallelize(data, 2)
rdd.glom().collect()

# COMMAND ----------

# data shuffled into different partitions
# increase number of partitions,
# using repartition we can also reduce partitions, but not recommedned, use coalsece to reduce parttition
# repartition distribute the data across systems , involve heavy shuffling
# more shuffling affect performance
rdd2 = rdd.repartition(4)
rdd2.glom().collect()

# COMMAND ----------

# to reduce the partitions, should use coalesce function
# coalesce - try its best to reduce the shuffling across executors
rdd3 = rdd.coalesce(1) 
rdd3.glom().collect()

# COMMAND ----------

