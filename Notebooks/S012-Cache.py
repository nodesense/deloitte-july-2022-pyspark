# Databricks notebook source
# cache 
#  cache the partition data
# partition is typically cleaned once stage is completed or task is completed 
# input partition data can be removed from memory once task is over/completed
# output partition data can be removed once shuffling /stage is completed
# we cache the partition data in the same executor

# why cache?

# Avoid reduntant I/O operations across many jobs in same spark application [each action create a job, a spark notebook may have more actions/jobs]
     # we have huge input files to read from HDFS/S3/ADLS etc.. over network or Disk [slow], expensive to fetch same again and again..
# Avoid reduntant CPU/Computation across many jobs in same spark application

# Reuse the RDD in multiple actions/jobs

# Cache Types
   # Memory - partition data stored in memory, if teh some partition data doesn't fit memory, then partition data is recomputed
   # Disk - data stored in Disk 
   # Memory + Disk = Some data stored in memory, fill over data, then stored into Disk
   # replication possible with Cache 
       # MEMORY 2 - teh cached data shall be stored in 2 executors memory in cluster 
       # DISK 2 - the cached data shall be stored in 2 executors disk in cluster
        
# DO NOT CACHE FACT DATA directory [multi millions of records or billions]
# CACHE Dimention [if small size]
# Cache the analytical results
# cache when RDD/DataFrame is reused [further compution jobs or reused to write results in csv/josn/parquet etc]

# COMMAND ----------

# how to remove special chars
# we will all ascii
def to_ascii(text):
    import re
    output = re.sub(r"[^a-zA-Z0-9 ]","",text)
    #print(output)
    return output

text = "prince, don't ;welcome"
to_ascii(text)

# COMMAND ----------

wordCountRdd = sc.textFile("/FileStore/tables/july2022/book_war_and_peace.txt")\
                 .map (lambda line: to_ascii(line))\
                  .map (lambda line: line.strip().lower())\
                 .map (lambda line: line.split(" "))\
                 .flatMap(lambda elements: elements)\
                 .filter (lambda word: word != "")\
                 .map (lambda word: (word, 1))\
                 .reduceByKey(lambda acc, value: acc + value)

# we are using wordCountRdd first time
from pyspark import StorageLevel

# wordCountRdd.persist(StorageLevel.MEMORY_AND_DISK)  # persist allow options DISK/Memory/Disk&MEmory etc
# cache/persist methods are lazy functions, cache is done only when first action applied on the specific RDD
wordCountRdd.cache() # will call persist internally with MEMORY only option
print("isCached ", wordCountRdd.is_cached)
wordCountRdd.take(10) # action 1, now result shall be cached

# COMMAND ----------

# use result from cache, no need to read file - Save IO
# no need to compute like cleansing, aggregation etc,
wordCountRdd.take(10)

# COMMAND ----------

