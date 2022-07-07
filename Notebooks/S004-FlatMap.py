# Databricks notebook source
# List of List of elements
data = [
    [1,2,3],
    [4,5,6],
    [7,8,9]
]

rdd = sc.parallelize(data)



# COMMAND ----------

rddMap = rdd.map ( lambda r: r ) # return list as is [1,2,3]
# let us see output of map 
# List of List of elements
print("Count ", rddMap.count()) # 3
rddMap.collect()

# COMMAND ----------

# flatMap
# flatten elements inside array into elements
# remove the list, project elements are records
# each element in the list will be 1 record
rddFlatMap = rdd.flatMap (lambda r: r) 
print ("Count ", rddFlatMap.count())
rddFlatMap.collect()

# COMMAND ----------

