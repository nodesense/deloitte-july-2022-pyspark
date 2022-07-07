# Databricks notebook source
# Databricks notebook source
# accumulator designed to fetch data from executor to notebook
# Databricks notebook source
# accumulator useful to collect data from executor to driver program
rdd = sc.parallelize(range(0, 20), 4) # 4 partitions

print("Data ", rdd.glom().collect())

sumAccum = sc.accumulator(0) # build in accumulator, number type

# add does sum values 0 (initial value) + 1 + 2 + 3 + ..+ 19
# executed for every element in partition , foreach executed inside executor
# usecase foreach to process 1 element at a time ie CRUD - create/update/delete record , 1 at a time
# add function by default sum the values, which can be changed by writing custom accumulator
rdd.foreach(lambda n: sumAccum.add(n))  # run inside executor

print("Acc value is ", sumAccum.value) # driver

# COMMAND ----------

# 0 is initial value, whenever call sumFirstValueInPartitionAccum.add(..), the values are summed in accumulator
sumFirstValueInPartitionAccum = sc.accumulator(0) # build in accumulator, create and return and return accumulator

# write a accumulator that picks first element in each partition and sum them up
def sumFirstElement(partitionItr):
  global sumFirstValueInPartitionAccum
  for n in partitionItr: # iterate elements in the partition one by one
    sumFirstValueInPartitionAccum.add(n)
    break # we process only 1st element in the partition and end the loop
    
# foreachPartition - batch process all elements in partition
rdd.foreachPartition(lambda itr: sumFirstElement(itr))

# sum of first elements in partitions [0 + 5 + 10 + 15] = 30
print ("sum of first elements ", sumFirstValueInPartitionAccum.value)

# COMMAND ----------

# custom accumulator
# collect first element in each parition [not to sum/count]
# zero, addInPlace are default functions will be invoked by accumulator 
from  pyspark.accumulators import AccumulatorParam
class ListItemParamAccumulator(AccumulatorParam):
  # initialize accumulator accumulator([]), v = [] empty list this will call zero function
  # keyword
  def zero(self, v):
    return [] # return list, empty list used when no seed value given initially
  # variable is list, value is the arg we pass via add function
  # acc.add(value) ==> calls addInPlace()
  # addInPlace is keyword fucntion, used whenever acc.add called
  # variable is list, value is passed from add function, acc.add(10), value = 10
  # we are adding value into list, [10, 20...], by default accumulator sum the values, here we collect values
  def addInPlace(self, variable, value):
    # fixme, we are muating list, fix to immutable    
    variable.append(value)
    return variable
  


# COMMAND ----------

# create accumulator
# [] is empty list
# ListItemParamAccumulator is custom accumulator defiend above
firstValueAccum = sc.accumulator([], ListItemParamAccumulator()) # calls zero function to initalize default empty []


# write a accumulator that picks first element in each partition and sum them up
def sampleFirstElement(partitionItr):
  global firstValueAccum
  for n in partitionItr: 
    # n is passed as value to addInPlace function
    firstValueAccum.add(n) # this will call ListItemParamAccumulator addInPlace function
    break
    
rdd.foreachPartition(lambda itr: sampleFirstElement(itr))

print (" first elements in each partition ", firstValueAccum.value) # value is list that contains first element from each partition, not number

# COMMAND ----------

