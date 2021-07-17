import pytest

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types
import os


@pytest.fixture(scope="module")
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
