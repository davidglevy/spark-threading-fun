# Databricks notebook source
import logging
import threading
import time

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)


format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                    datefmt="%H:%M:%S")

logging.info("Main    : before creating thread")
x = threading.Thread(target=thread_function, args=(1,))
logging.info("Main    : before running thread")
x.start()
logging.info("Main    : wait for the thread to finish")
x.join()
logging.info("Main    : all done")

# COMMAND ----------

# MAGIC %md
# MAGIC # Threading in UDF
# MAGIC Now we'll confirm we're able to call the threading code

# COMMAND ----------

from pyspark.sql.functions import udf, StringType, ArrayType, lit

# simple countdown latch, starts closed then opens once count is reached
class CountDownLatch():
    # constructor
    def __init__(self, count):
        # store the count
        self.count = count
        # control access to the count and notify when latch is open
        self.condition = Condition()
 
    # count down the latch by one increment
    def count_down(self):
        # acquire the lock on the condition
        with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return
            # decrement the counter
            self.count -= 1
            # check if the latch is now open
            if self.count == 0:
                # notify all waiting threads that the latch is open
                self.condition.notify_all()
 
    # wait for the latch to open
    def wait(self):
        # acquire the lock on the condition
        with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return
            # wait to be notified when the latch is open
            self.condition.wait()

def run_threads(thread_names):
    thread_handles = []
    for name in thread_names:
        handle = threading.Thread(target=thread_function, args=(name,))
        logging.info(f"Creating thread {name}")
        thread_handles.append(handle)
        handle.start()
    
    logging.info("All threads started")
    
    f

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, desc, row_number, collect_list, struct
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window


list_of_stuff = [
    {"id": "a", "url": "AAAA"},
    {"id": "b", "url": "BBBB"},
    {"id": "c", "url": "CCCC"},
    {"id": "d", "url": "DDDD"},
    {"id": "e", "url": "EEEE"},
    {"id": "f", "url": "FFFF"},
    {"id": "g", "url": "GGGG"},
    {"id": "h", "url": "HHHH"},
    {"id": "i", "url": "IIII"},
    {"id": "j", "url": "JJJJ"},
    {"id": "k", "url": "KKKK"},
    {"id": "l", "url": "LLLL"},
    {"id": "m", "url": "MMMM"},
    {"id": "n", "url": "NNNN"},
    {"id": "o", "url": "OOOO"},
    {"id": "p", "url": "PPPP"},
    {"id": "q", "url": "QQQQ"},
    {"id": "r", "url": "RRRR"},
    {"id": "s", "url": "SSSS"},
    {"id": "t", "url": "TTTT"},
    {"id": "u", "url": "UUUU"},
    {"id": "v", "url": "VVVV"},
    {"id": "w", "url": "WWWW"},
    {"id": "x", "url": "XXXX"},
    {"id": "y", "url": "YYYY"},
    {"id": "z", "url": "ZZZZ"}
]

schema = StructType([
    StructField("id", StringType(), False),
        StructField("url", StringType(), False)
])

df = spark.createDataFrame(list_of_stuff, schema)



df_index = df.withColumn('index_column_name', (row_number().over(Window.orderBy(monotonically_increasing_id())) - 1) % 5).groupBy("index_column_name").agg(collect_list(struct('id', 'url')).alias("urls_to_run"))

# COMMAND ----------

display(df_index)
