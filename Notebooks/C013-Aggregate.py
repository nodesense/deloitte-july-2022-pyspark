# Databricks notebook source
# aggregate - custom aggregate method beyond min,max,avg,sum,count etc
# code is from spark doc

# sequence operation is applied on partition data and get an aggregate result per partition
# partial aggregation per partition
# x is tuple, x[0] may be sum of numbers, x[1] may be count of number in each partitions [upto logic ]
seqOp = (lambda x, y: (x[0] + y, x[1] + 1)) 
# combining operations is applied on top of result of aggregated partition result by seqOp
# final output 
# final aggregation by merging all partition output to generate final output
combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1])) # combining operations
# aggregate is action method
sc.parallelize([1, 2, 3]).aggregate((0, 0), seqOp, combOp)

# COMMAND ----------

def sOp(x, y):
    print("sOP", x, y)
    # x is tuple, x[0] is sum value, x[1] is count  value
    return (x[0] + y, x[1] + 1) # return tuple
  
def cOp(x, y):
    print ("cOp", x, y)
    # x is tuple, x[0] is sum value, x[1] is count value, since it is combine func, we sum total result
    return (x[0] + y[0], x[1] + y[1]) # return tuple
    
seqOp = (lambda x, y: sOp(x, y) )
combOp = (lambda x, y: cOp(x, y))
"""
(0, 0)  -- aggregate initial value 
            first   0 - represent sum of values
            second 0 - represent count of values
            
seqOp - Sequence operation is executed first at all teh partitions
combOp - Combine operations - executed last , result of SeqOp is passed as input to combine Operation
"""

# combOp is executed in driver after aggregation seqOp from executor
# seQqOp executed in executor on each partitions
# partition 0 [1, 2, 3, 4], seq is applied on here , output is (10, 4) where is 10 is sum, 4 count
# partition  1 [4, 5, 6, 7, 8,9], seq is applied on here, output is (35, 5) where as 35 is sum, 5 is count
# driver shall collect results (10, 4), (35, 5) from each executors, apply combOp on top of them
# comb (10 + 35, 4 + 5) => (45, 9) where 45 is sum of all numbers, 9 is count of all numbers
result = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8,9], 2).aggregate((0, 0), seqOp, combOp)
print("result ", result)

# COMMAND ----------

