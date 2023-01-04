# Databricks notebook source
dbutils.widgets.dropdown("runTests", "Yes", ["No", "Yes"], label="Run Tests?")


# COMMAND ----------

import time
import random
import logging
import threading

random.seed()


def run_wait(row_id):
    
    start = time.time()
    
    print(f"Starting row [{row_id}] wait with slight variation")
    
    time.sleep(3 + random.randint(0,2))
    
    print(f"Ending row [{row_id}] wait with slight variation")

    end = time.time()

    return int(end - start)

runTests = dbutils.widgets.get("runTests")

if (runTests == "Yes"):
    print("Running single test")
    print("Duration was: " + str(run_wait(1)))

# COMMAND ----------

from pyspark.sql.functions import udf, max,col
from pyspark.sql.types import IntegerType

values = []
for x in range(1,100):
    values.append({'row_id': x})
    
df = spark.createDataFrame(values, "row_id int")

run_wait_udf = udf(run_wait, IntegerType())

# COMMAND ----------

def runUdfWithWait():
    start = time.time()

    df_results = df.withColumn("run_time", run_wait_udf("row_id")).agg(max(col("run_time")).alias('longest_time'))

    results = df_results.collect()
    end = time.time()

    total_time = end - start
    
    
    longest = results[0]['longest_time']

    print(f"The total time was [{total_time}]")
    print(f"The longest thread was [{longest}]")
    
    print(results)    

# COMMAND ----------

if runTests == "Yes":
    runUdfWithWait()


