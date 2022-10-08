# Databricks notebook source
# MAGIC %md
# MAGIC # Experiment: Increase Executor Cores
# MAGIC 
# MAGIC The first thing we tried to do was increase the number of "slots" per executor. This did not work as intended, I tried a few different property changes and each would stop the start of the cluster.
# MAGIC 
# MAGIC It appears the Databricks clusters are really tightly configured to run 1 core per 1 vCPU. Attempts to double the number of executor cores failed. I also attempted to decrease the 
# MAGIC 
# MAGIC Attempts:
# MAGIC 
# MAGIC 1. Changed "spark.executor.cores" to 16 (was 8) --> FAILURE: Jobs do not run, cluster shows "no executors"
# MAGIC 2. Changed "spark.task.cpus" to 0.5 --> FAILURE: Cluster does not start
# MAGIC 3. Repartitioned data frame with "repartition(20)" --> FAILURE: No Change to Concurrency
