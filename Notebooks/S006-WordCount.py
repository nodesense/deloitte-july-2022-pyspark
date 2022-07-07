# Databricks notebook source
# /FileStore/tables/july2022/words.txt

# Lazy evaluation, files won't be read until some action is applied on data
fileRdd = sc.textFile("/FileStore/tables/july2022/words.txt")

# COMMAND ----------

print (fileRdd.getNumPartitions())

# COMMAND ----------

# Count is an action method, it has read file, get the count from executors
# the files shall be read from hdfs by executor, load content into partitions, get the count
fileRdd.count()

# COMMAND ----------

# first action function
fileRdd.first()

# COMMAND ----------

# collect is a action method, this also create job, read data from hdfs etc
fileRdd.collect()

# COMMAND ----------

# RDD Lineage
# Map is transformation ,lazy evaluation, no job is created
lowerCaseRdd = fileRdd.map (lambda line: line.lower().strip())

# COMMAND ----------

# collect is action, creates job, load files, read file, remove space
lowerCaseRdd.collect()

# COMMAND ----------

wordListRdd = lowerCaseRdd.map (lambda line: line.split(" "))

# COMMAND ----------

# words list
wordListRdd.collect()

# COMMAND ----------

# flatMap, remove the list, project element in the list as record
wordRdd = wordListRdd.flatMap(lambda elements: elements)

# COMMAND ----------

wordRdd.collect()


# COMMAND ----------

wordRdd =  wordRdd.filter (lambda word: word != "")
wordRdd.collect()

# COMMAND ----------

# convert word into (key,value) rdd (spark, 1) for reduceByKey
pairRdd = wordRdd.map (lambda word: (word, 1))

# COMMAND ----------

pairRdd.collect()

# COMMAND ----------

# get word count using reduceByKey
# transformation
wordCountRdd = pairRdd.reduceByKey(lambda acc, value: acc + value)

# COMMAND ----------

wordCountRdd.collect()

# COMMAND ----------

# executor shall write partition data into directory per partitions
wordCountRdd.saveAsTextFile ("/FileStore/tables/july2022/word-could-results")

# COMMAND ----------

