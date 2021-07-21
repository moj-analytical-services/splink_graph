import networkx as nx
import pyspark
import warnings
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
import networkx as nx
from splink_graph.node_metrics import eigencentrality


def _find_graphframes_jars(spark: SparkSession):
    try:
        from graphframes import GraphFrame
    except ImportError:
        raise ImportError(
            "need to import graphframes. ie from graphframes import Graphframes"
            "if you dont have it you need first to install graphframes from PyPI"
            "i.e.  pip install graphframes==0.6.0 "
        )

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


def sp_connected_components(
    edges_df,
    src="src",
    dst="dst",
    weight_colname="tf_adjusted_match_prob",
    cluster_id_colname="cluster_id",
    cc_threshold=0.90,
):

    edges_for_cc = edges_df.filter(f.col(weight_colname) > cc_threshold)

    nsrc = edges_for_cc.select(src).withColumnRenamed(src, "id")
    ndst = edges_for_cc.select(dst).withColumnRenamed(dst, "id")
    nodes_for_cc = nsrc.union(ndst).distinct()

    g = GraphFrame(nodes_for_cc, edges_for_cc)
    cc = g.connectedComponents()

    cc = cc.withColumnRenamed("component", "cluster_id").withColumnRenamed(
        "id", "node_id"
    )

    return cc
