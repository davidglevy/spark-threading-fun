# Databricks notebook source
# MAGIC %md
# MAGIC # 02.2 Experiment - Switch to a Pandas UDF

# COMMAND ----------

# MAGIC %run "./01 Problem Illustration" $runTests=No

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aim
# MAGIC We intend to use Pandas Scalar UDF function to take a Pandas series (URLs) and pass this multi-row object to our own Thread Pool executor. We will allow 10 "workers" per function invocation but this can likely be increased for IO bound workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Methodology
# MAGIC Within our Pandas UDF, we'll take the input series of URLs, creating a ThreadPoolExecutor. We'll then use these via futures to perform each request in parallel and utilise a CountDownLatch to join the threads once all work is finished. We'll return an object which will be a struct with the url and the result.
# MAGIC 
# MAGIC For this test, we'll not actually invoke the external REST service but still perform a wait and return a foo result.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, StringType
import pandas as pd
from pandas import Series

# Use pandas_udf to define a Pandas UDF
# @pandas_udf()
# Input/output are both a pandas.Series of doubles

@pandas_udf(IntegerType())
def pandas_plus_one(v: Series) -> Series:
    return v + 1



# COMMAND ----------

data_values = [
    {'v': 1.0},
    {'v': 2.0},
    {'v': 3.0},
    {'v': 4.0}
]

df = spark.createDataFrame(data_values, "v double")
result_df = df.withColumn('v2', pandas_plus_one(df.v))

# COMMAND ----------

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType



# Lets test that we can operate on each element for one invocation.
@pandas_udf(StringType())
def pandas_string_op(v: Series) -> Series:
    all_data = v.to_list()
    
    other_cells = ""
    
    temp = []
    for element in all_data:
        if (other_cells != ""):
            other_cells = other_cells + " "
        other_cells = other_cells + element
        temp.append(element + "_mutated")

    results = []
    for t in temp:
        results.append(t + " " + other_cells)
    
    return pd.Series(results)


# COMMAND ----------

data_values = [
    {'v': "a"},
    {'v': "b"},
    {'v': "c"},
    {'v': "d"},
    {'v': "e"},
    {'v': "f"},
    {'v': "g"},
    {'v': "h"},
    {'v': "i"},
    {'v': "j"},
    {'v': "k"},
    {'v': "l"},
    {'v': "a1"},
    {'v': "b1"},
    {'v': "c1"},
    {'v': "d1"},
    {'v': "e1"},
    {'v': "f1"},
    {'v': "g1"},
    {'v': "h1"},
    {'v': "i1"},
    {'v': "j1"},
    {'v': "k1"},
    {'v': "l1"},
    {'v': "a2"},
    {'v': "b2"},
    {'v': "c2"},
    {'v': "d2"},
    {'v': "e2"},
    {'v': "f2"},
    {'v': "g2"},
    {'v': "h2"},
    {'v': "i2"},
    {'v': "j2"},
    {'v': "k2"},
    {'v': "l2"}
]

original_string_df = spark.createDataFrame(data_values, "v string")
result_string_df = original_string_df.withColumn('v2', pandas_string_op(original_string_df.v))
display(result_string_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partial Success
# MAGIC At this point, we've shown that the Pandas UDF is exposed to multiple rows, but the split wasn't even across each executor, some had 4 whilst others had 2. This will result in skew but should even out with larger data sets.
# MAGIC 
# MAGIC The next step is to create a Pandas UDF which uses a ThreadPoolExecutor

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

# For local test, we'll create 40 threads.
# When run in Spark, we'll reduce this to 10, as that will still be many threads with the executor as a
# multiplier.
n_threads = 40

def run_wait_wrapped(row_id):
    return (run_wait(row_id), row_id)


with ThreadPoolExecutor(n_threads) as executor:
    futures = [executor.submit(run_wait_wrapped, value) for value in values]

    for future in as_completed(futures):
                # get the downloaded url data
                duration, data = future.result()
                print(f"Row {data['row_id']} finished in {duration} seconds")
    




# COMMAND ----------

# Lets test that we can operate on each element for one invocation.
@pandas_udf(IntegerType())
def pandas_run_wait(v: Series) -> Series:

    # Initialize a new ThreadPoolExecutor.
    # For IO Bound workloads, there can be many many more threads than cores, but for this we'll limit it to 10.
    
    # Get all the data passed to this function.
    # Order here is important to ensure they are passed back correctly.
    row_ids = v.to_list()
    
    results_dict = {}
    n_threads = 100

    with ThreadPoolExecutor(n_threads) as executor:
        futures = [executor.submit(run_wait_wrapped, row_id) for row_id in row_ids]

        for future in as_completed(futures):
                    # get the downloaded url data
                    duration, row_id = future.result()

                    # Use a map for efficient lookup later.
                    results_dict[str(row_id)] = duration
    results = []
    for row_id in row_ids:
        results.append(results_dict[str(row_id)])
    
    return pd.Series(results)


# COMMAND ----------

from pyspark.sql.functions import udf, max,col
from pyspark.sql.types import IntegerType

values = []
for x in range(1,201):
    values.append({'row_id': x})
    
df = spark.createDataFrame(values, "row_id int")

# COMMAND ----------

import time

st = time.time()
result_df = df.repartition(8).withColumn('duration', pandas_run_wait(col("row_id")))

display(result_df)
et = time.time()
# get the execution time
elapsed_time_threaded = et - st
print('Time to run with threading:', elapsed_time_threaded, 'seconds')

# COMMAND ----------

st = time.time()
df_results = df.withColumn("run_time", run_wait_udf("row_id"))
display(df_results)
et = time.time()
# get the execution time
elapsed_time_normal = et - st
print('Time to run without threading:', elapsed_time_normal, 'seconds')

# COMMAND ----------

percent_reduction = round(elapsed_time_threaded / elapsed_time_normal * 100, 2)
print(f"Comparative Runtime: {percent_reduction}%")
