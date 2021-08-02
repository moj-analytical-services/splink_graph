import networkx as nx
import pyspark
import warnings
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pandas as pd
from graphframes import GraphFrame
from splink_graph.utils import nodes_from_edge_df
import os


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


def graphframes_connected_components(
    edges_df,
    src="src",
    dst="dst",
    weight_colname="tf_adjusted_match_prob",
    cluster_id_colname="cluster_id",
    cc_threshold=0.90,
):

    edges_for_cc = edges_df.filter(f.col(weight_colname) > cc_threshold)
    nodes_for_cc = nodes_from_edge_df(
        edges_for_cc, src="src", dst="dst", id_colname="id"
    )

    g = GraphFrame(nodes_for_cc, edges_for_cc)
    cc = g.connectedComponents()

    cc = cc.withColumnRenamed("component", cluster_id_colname).withColumnRenamed(
        "id", "node_id"
    )

    return cc


def nx_connected_components(
    spark: SparkSession,
    edges_df,
    src="src",
    dst="dst",
    weight_colname="tf_adjusted_match_prob",
    cluster_id_colname="cluster_id",
    cc_threshold=0.90,
    edgelistdir=None,
):

    pdf = edges_df.toPandas()

    filtered_pdf = pdf[pdf[weight_colname] > cc_threshold]

    nxGraph = nx.Graph()
    nxGraph = nx.from_pandas_edgelist(filtered_pdf, src, dst, weight_colname)

    netid = 0
    cclist = []
    idlist = []

    for c in nx.connected_components(nxGraph):
        netid = netid + 1 # go to next conn comp id

        # create a graph from this connected component
        currentnx = nxGraph.subgraph(c)

        # if we need to write the edgelist of the component to a directory do so. 
        if edgelistdir is not None:
            os.makedirs(edgelistdir, exist_ok=True)
            nx.write_edgelist(
                currentnx, edgelistdir + "/cluster" + str(netid).zfill(9) + "edgelist"
            )

        # append the current cc id and the node id for this component
        for n in currentnx.nodes():
            idlist.append(n)
            cclist.append(str(netid).zfill(9))

    out = pd.DataFrame(zip(cclist, idlist), columns=["cluster_id", "node_id"])

    return spark.createDataFrame(out)
