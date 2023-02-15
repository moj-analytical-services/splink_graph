import pytest
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types
import os
from functools import lru_cache


@lru_cache(maxsize=None)
@pytest.fixture(scope="function")
def spark():

    conf = SparkConf()

    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "24")

    if (pyspark.__version__).startswith("2"):
        conf.set("spark.sql.execution.arrow.enabled", "true")
        conf.set("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
        os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
    elif (pyspark.__version__).startswith("3"):
        conf.set(
            "spark.driver.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
        conf.set(
            "spark.executor.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)

    SPARK_EXISTS = True

    yield spark


@pytest.fixture(scope="function")
def sparkSessionwithgraphframes():

    conf = SparkConf()

    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "24")
    conf.set("spark.jars", "jars/graphframes-0.8.2-spark3.1-s_2.12.jar")
    if pyspark.__version__ .startswith("3"):
        conf.set(
            "spark.driver.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
        conf.set(
            "spark.executor.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
        

    sc = SparkContext.getOrCreate(conf=conf)

    sparkSessionwithgraphframes = SparkSession(sc)

    SPARK_EXISTS = True

    yield sparkSessionwithgraphframes


@pytest.fixture(scope="function")
def graphframes_tmpdir(tmpdir_factory):
    return tmpdir_factory.mktemp("graphframes_tempdir")
