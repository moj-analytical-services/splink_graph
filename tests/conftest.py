import pytest

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types


@pytest.fixture(scope="module")
def spark():

    conf = SparkConf()

    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "24")

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)

    SPARK_EXISTS = True

    yield spark
