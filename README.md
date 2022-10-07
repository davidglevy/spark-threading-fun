# spark-threading-fun
A small repository for doing various experiments with threading to solve "Embarrassingly Parallel" (IO Bound) concurrency.

I hit this issue whilst I was working on a Spark UDF which was invoking an external REST API. Back when I was a Java
developer, I'd go straight to a ThreadPool. Looking at options which will solve this using a more Spark focused approach.


# Thoughts
It looks like the operation is bound by the number of active tasks in a stage. Default execution within Databricks
keeps this as a 1:1 with the number of cores on the cluster.

# Experiment 1: Increase the number of Executor Cores
For this experiment I'll look at changing Spark settings and ensuring the data is
partitioned (in Spark Data Frame) to allow more parallel execution.


# Experiment 2: Switch to a Pandas UDF
I saw this link https://dataninjago.com/2019/05/11/handling-embarrassing-parallel-workload-with-databricks-notebook-workflows/ which indicates that a Pandas UDF will allow more parallel execution by sending in a Pandas Dataframe which I might be able to run a bulk "apply" method on.


# Experiment 3: Break data into "Execution Groups"
Another approach would be to take the work of multi-threading within an executor core. Whilst this isn't likely palatable for some, an IO bound problem requires far more threads than normally executed stages. This option will be a good look if Experiment 1 doesn't bear fruit.