import networkx as nx
import numpy as np
import pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.types import LongType, StringType, FloatType, DoubleType

import pyspark
import warnings
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
import networkx as nx
from graphframes import GraphFrame


def _find_graphframes_jars(spark: SparkSession):

    sparkjars_conf = spark.conf.get("spark.jars")

    if "graphframes" not in sparkjars_conf:
        warnings.warn(
            "You need to include the path to the graphframes jar to your spark.jars conf settings"
            "when you define your 'sparkSession'"
            "Check related documentation on INSTALL.md on the splink_graph repo"
        )
        return 1

    if "logging" not in sparkjars_conf:
        warnings.warn(
            "You need to include the path to `scala-logging-api` and `scala-logging-slf4j` "
            "to your spark.jars conf settings when you define your 'sparkSession' "
            "Check related documentation on INSTALL.md on the splink_graph repo"
        )
        return 2

    return 0
