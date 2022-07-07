# Databricks notebook source
# a reference or look up data, shall be marshalled (coping)/serialization into executor, will impact performance every time when task executed
# we can avoid marshalling(coping) for each task by using broadcast
# broadcast will ensure data is there in executor cache one time
sector_dict = {
        "MSFT": "TECH",
        "TSLA": "AUTO",
        "EMR": "INDUSTRIAL"
}

# this stock data shall goes into RDD, into partitiion
stocks = [
    ("EMR", 52.0),
    ("TSLA", 300.0),
    ("MSFT", 100.0)
]

# COMMAND ----------

# create broadcasted variabel using Spark Context
# this will ensure that sector_dict is kept in every executor 
# where task shall be running
# lazy evaluation, data shall be copied to executors when we run the job
# broadCastSectorDict is broad cast data
# where as sector_dict is python dict which is not broadcasted
# if the code uses sector_dict, then sector_dict is copied into executor every time task is executed, ie 1000 tasks, 1000 data copied
# if the code uses broadCastSectorDict, then broadCastSectorDict is copied ONLY ONCe to executor, not for every task
broadCastSectorDict = sc.broadcast(sector_dict)

# COMMAND ----------

stocksRdd = sc.parallelize(stocks)

# COMMAND ----------

# Pyspark see this code, this has reference to broadCastSectorDict
# which is broardcast data, pyspark place the broadCastSectorDict in every executor
# 1 time instead of every job
# without broadCast, sector_dict shall be copied to executor for every task
# add sector code with stock at executor level when task running
# Task which uses broadCastSectorDict
def enrichStockWithSector(stock):
    return stock + (broadCastSectorDict.value[stock[0]] ,)

# COMMAND ----------

# code marshalling - Python copy the task lambda code to executor system/processor
# now enrichStockWithSector also shipped to executor on every task
enrichedRdd = stocksRdd.map (lambda stock: enrichStockWithSector(stock))

# COMMAND ----------

enrichedRdd.collect()

# COMMAND ----------

