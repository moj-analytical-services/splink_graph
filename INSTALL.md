# How to install splink_graph

The easiest way to install splink_graph is to type

`pip install --upgrade splink_graph`

on your terminal


There are some dependencies such as `numpy` that needs to be version "1.19.5" 
and `scipy` needs to be ">= 1.6.0"

But hopefully these are taken care of automatically when the package is installed.


There is a more important dependency on pyarrow for Pyspark 2.4.x that is discussed below

splink_graph also assumes the existance of PySpark (either 2.3.x/2.3.4 or 3.x) but this is not an enforcable dependency.
Without Pyspark however splink_graph will not work.

## Configuration details 



### Using Pandas UDFs in Python in Pyspark 2.4.x : prerequisites


This package uses Pandas UDFs for certain functionality.Pandas UDFs are built on top of Apache Arrow and bring 
the best of both worlds: the ability to define low-overhead, high-performance UDFs entirely in Python.

With Apache Arrow, it is possible to exchange data directly between JVM and Python driver/executors with near-zero (de)serialization cost.
However there are some things to be aware of if you want to use these functions.
Since Arrow 0.15.0, a change in the binary IPC format requires an environment variable to be compatible with previous versions of Arrow <= 0.14.1. 

This is only necessary to do for PySpark users with versions 2.3.x and 2.4.x that have manually upgraded PyArrow to 0.15.0. The following can be added to conf/spark-env.sh to use the legacy Arrow IPC format:

    ARROW_PRE_0_15_IPC_FORMAT=1

Another way is to put the following on spark .config

    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")


This will instruct PyArrow >= 0.15.0 to use the legacy IPC format with the older Arrow Java that is in Spark 2.3.x and 2.4.x. Not setting this environment variable will lead to a similar error as described in [SPARK-29367](https://issues.apache.org/jira/browse/SPARK-29367) when running pandas_udfs or toPandas() with Arrow enabled.


So all in all : either PyArrow needs to be at most in version 0.14.1 or if that cannot happen the above settings need to be be active.



### Using Pandas UDFs in Python in Pyspark 3.x

No need for any special configuration.


## Testing

In order to test splink_graph if you have cloned splink_grap please run 
`pytest -v` on a terminal while located at the root folder of the repo.
