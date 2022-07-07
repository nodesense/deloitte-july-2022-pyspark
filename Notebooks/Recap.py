# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Day 1
# MAGIC 
# MAGIC 1. Apache Spark Introduction
# MAGIC 2. Distributed computing
# MAGIC 3. Parallel computing [work on indepedent data set or partition without rely any other concurrency]
# MAGIC 4. Spark is In Memory Computing
# MAGIC 5. Cluster Components: Driver [your application], Cluster manager [resource manager], Worker, Executor
# MAGIC 6. Driver and Executor work together to get the job done
# MAGIC 7. Resource manager/cluster manager and worker allocate resources in terms CPU, RAM
# MAGIC 
# MAGIC 8. Spark Architecture [Spark Core, Spark SQL, Spark Stream, ML, GraphX]
# MAGIC 9. Spark core is foundation [RDD, Partition, , Transformation, Action, Job, DAG, Stage, Task, ]
# MAGIC 10. RDD - Resilient Distributed Dataset [Creation, Transformation, Action]
# MAGIC 11. RDD is a tree, lineage, RDD constains low level spark code, has Partitions of Data to operate on
# MAGIC 12. RDD creation can be done parallize method [load in memory data into RDD from driver to Executor] or RDD can be created from files using IO
# MAGIC 13. Data involved in RDD is split into partitions [provide parallelism], partition is immutable, once created, we cannot modify/delete/insert/update partition data
# MAGIC 14. Each partition shall have a task [task may be filter, map, flatmap, join etc along with developer code in python, scala, java]
# MAGIC 15. Narrow and Wider Transformation, Narrow Transformation means, with in stage, without shuffling, data has been transformed.  Wider tranformation, involves shuffling across machine [groupby, join etc]
# MAGIC 
# MAGIC 16. Every action [collect, show, fold, ...] creates JOB. Job is created inside driver not in executor. 
# MAGIC 17. Each Job, internally convert RDD into DAGs, DAGs also scheduled
# MAGIC 18. Each DAG is planned to be a  Stage for execution, Stage 0, Stage 1,... spark should complete stage before moving next stage, partition data in stage shall be cleaned one next stage started execution
# MAGIC 19. Each stage shall have tasks [map, filter etc] which are scheduled by Task Scheduler
# MAGIC 20. Task Scheduler with in Spark Driver send the task to executor, File I/O modules read/write files in executor, Block Manager with in Executor responsible for Partition, Shuffling etc

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 2: Spark SQL
# MAGIC 
# MAGIC 1. Spark Session which is entry point for Spark SQL
# MAGIC 2. DataFrame/Spark SQL is built on top of Spare core, Dataframe is built on top of RDD
# MAGIC 3. DataFrame is Strutured data where RDD is unstructured data
# MAGIC 4. Structured Data means, Data with  meta data, Data is row of data where meta data is schema, schema shall have columns, columns shall have name, data type, nullable or not
# MAGIC 5. Spark DataFrame support nested schema , schema inside schema , customer data may consist of address which has more attributes like street, city, pincode etc
# MAGIC 6. DataFrame and Spark sql are APIs on top of DataSet
# MAGIC 7. Two types of API, untyped API and Typed API
# MAGIC 8. Untyped API used with Spark SQL, PySpark, R
# MAGIC 9. Typed API used with Scala, Java [JVM compiled Languages]
# MAGIC 10. While writing Data Frame code, or SQL code, The DF  code and SQL codes are parsed, converted into AST [Abstract Syntax Tree]
# MAGIC 11. AST is analysed , Unresolved Logical Plan , unrolved logical plan, no table, or column existance or column  type compatiblity checked, your code as is converted into Unresovled Logical Plan
# MAGIC 12. From Unresolved Logical Plan, Catalog is applied [Hive Catalog, spark schemas], then it validates table, column existance, data type compatiblity , then create a Logical Plan [not optimized ], code as is
# MAGIC 13. From Logical Plan, it creates Optimized Logical Plan 
# MAGIC     a. Input table may have 20 columns, but spark see what are columns used, only pick required columns 
# MAGIC     b. if joins or predicates, null conditions  example:  age > 20 [age is not null and age > 20]
# MAGIC     c. Sort is used, Filter is used, it apply filter first, then sort
# MAGIC     
# MAGIC 14. Optimized plans used to create Physical plan [cost model for joins]
# MAGIC 15. RDD Code is generated for Seleted Physical plan [Scala quasi quotes]
# MAGIC 16. Quosi quotes used to generate Scala code by libraries
# MAGIC 17. Scala is compiled to generate JVM byte code
# MAGIC 18. Byte code is executed by JVM

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. Analytics API
# MAGIC 2. Window Functions
# MAGIC 3. Joins
# MAGIC 4. Parquet
# MAGIC 5. Data Lakes 
# MAGIC 6. Delta Lakes
# MAGIC 7. Spark SQL
# MAGIC 8. JDBC [Derby DB]