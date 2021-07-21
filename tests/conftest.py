import pytest

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types
import os


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

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)

    SPARK_EXISTS = True

    yield spark


@pytest.fixture(scope="function")
def sparkwithgraphframes():

    conf = SparkConf()

    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "24")
    

    if (pyspark.__version__).startswith("2"):
        conf.set("spark.sql.execution.arrow.enabled", "true")
        conf.set("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
        conf.set("spark.jars", "jars/graphframes-0.6.0-spark2.3-s_2.11.jar,jars/scala-logging-api_2.11-2.1.2.jar,jars/scala-logging-slf4j_2.11-2.1.2.jar")
        os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
    else:
        conf.set("spark.jars", "jars/graphframes-0.8.0-spark3.0-s_2.12.jar")

    sc = SparkContext.getOrCreate(conf=conf)

    sparkwithgraphframes = SparkSession(sc)

    SPARK_EXISTS = True

    yield sparkwithgraphframes
    
    

@pytest.fixture(scope = 'function')
def graphframes_tmpdir(tmpdir_factory):
    return tmpdir_factory.mktemp('graphframes_tempdir')
