# Databricks notebook source
# foreach 
# apply a function to each element in the rdd [all partitions]
# write any custom functions that deal with db, datastore, cache etc


orders = [
    # symbol, qty
    ('INFY', 200),
    ('TSLA', 50),
    ('EMR', 20),
    ('INFY', 100),
    ('TSLA', 25)
]

def add(acc, value):
    output = acc + value
    print("acc", acc, "value", value, "output", output)
    return output

orderRdd = sc.parallelize(orders, 2)
# refer S015-Fold for example,
resultRdd = orderRdd.foldByKey(0, add)
resultRdd.collect() # now we take result to driver, which is not efficient
# in spark, driver is not distributed, not scalable, runs on only one system
# the executor is distriuted, runs across clouster, parallel

# COMMAND ----------

# foreach vs map 
   # foreach is action method, doesn't return values, iterate values one by one in the partition, runs inside executor
   # map is tranformation method, returns transformed value [n input,s n outputs]
    
# now let us process the data at executor using forEach
# resultRdd has result, now we apply foreach, it may store data to data store
# good for processing ONE RESULT at a time
# foreach executed in executor process, not in driver
def updateDB(stock):
    #Todo, update, insert, delete record into DB
    # CRUD API, POST, DELETE, PUT with REST API
    print("Saving ", str(stock), " to db ")
    
# foreach is ACTION method, runs on executor, updateDB function is called over every item resultRdd
resultRdd.foreach(updateDB)

# COMMAND ----------

# foreachPartition
# custom logic to handle data in the partitions
# runs inside executors
# foreach process 1 element at a time, 
# where as foreachPartition can process each  partition data as batch 
# bulk insert/update/delete

# iterator will have each partition data as whole
# part0 - 5 records, then iterator shall have 5 records
# processData is called by foreachPartition on executor for each partition
# iterator passed for foreachPartition
def processResultData(iterator):
    print("Process data called ")
    for record in iterator:
        print ("Processing ", str(record))
        
    print ('-' * 30)
# Action method
resultRdd.foreachPartition(processResultData)

# COMMAND ----------



# COMMAND ----------

