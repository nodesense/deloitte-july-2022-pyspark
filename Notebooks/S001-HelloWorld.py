# Databricks notebook source
# this is called cell
# python code, not spark, runs inside spark driver not in executor
print ("Hello spark")
# use SHIFT + ENTER to run shell

# COMMAND ----------

# in Data Brick  notebooks, Spark Context already created, avaiable as sc
# do not write comments
sc
# Master local means embedded mode, spark driver, spark executor runs in same JVM process only for dev
# local[8] = number of parallel tasks that can be run at same TIME [NOT TOTAL TASKS in SPARK]
# each core = 4 task as time
# 2 cores = 8 tasks

# COMMAND ----------

# use spark context to create RDD - Resillient Distributed Data Set
# RDD is composed as Tree - lineage [parent, child, grand child .........]
# RDD is lower level code, used for working with unstructured data [txt]
# RDD creation is lazy process [doesn't involve executor]
# RDD execution shall use clusters , executors

# CREATE RDD of numbers
# python list
# this data in Driver memory, not in executor
data = [1,2,3,4,5,6,7,8,9,10]

# LAZY evaluation, load the data into spark executor partitions
# right now, parallize shall not load data until an ACTION applied
rdd = sc.parallelize(data)
# doesn't use executor yet, note, no job/job id printed here below cell

# COMMAND ----------

# TRANSFORMATION - LAZY - spark task applied on RDD partitition data at executor
# LAzy evaluation, no executor is used until we apply ACTION
# filter is higher order function, accept lambda as parameter, and 
# filter shall call lambda for each number 1,2...10 [10 times], 
# lambda either return true or false
# fitler picks 1, 3, 5, 7, 9 [they returend true/odd number]
odds = rdd.filter (lambda n: n % 2 == 1) # pick only odd number

# odds is child rdd, rdd is parent rdd, they form lineage, create TREE

# COMMAND ----------

# ACTION - EXECUTION - PHYSICAL EXECUTION ON Executor
# collect is an ACTION function, which will execute RDD tree, will use executors
# collect , collects results from all partitions across many systems if any
# result is python list, available in spark driver
result = odds.collect()

print(result) # print all odd numbers
# note the Spark Jobs displayed below shell 
# Job 0 - 0 is job id

# COMMAND ----------

# what happen when ACTION applied on RDD
# Action create a JOB
# JOB convert the RDD tree into DAG graphs
# DAG is converted into Stages using DAG Schduler
# Task are created which can be run inside a stage
# Tasks are schduled for execution by Task Scheduler located in Spark Driver, 
# Tasks are submitted to Executor
# Executor execute the task [results are stored in executors partition]
# Finally results are accumulated to Driver from executor

# COMMAND ----------

# take functions take(n) better than collect as it picks only n results instead all results

# take - action function
result = odds.take(2) # 1, 3

print(result)

# COMMAND ----------

# min is action
# notebooks print result of last expression
odds.min()

# COMMAND ----------

# max - action 
odds.max()

# COMMAND ----------

# count - action
odds.count() # number of elements

# COMMAND ----------

odds.sum() # action 1 + 3 + 5 + 7 + 9

# COMMAND ----------

