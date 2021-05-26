import pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import when
import networkx as nx
from pyspark.sql.types import LongType, StringType, FloatType, DoubleType
from networkx import *


def _graphharmoniser(df, colsrc, coldst):
    df = df.withColumn(
        "newsrc",
        when(f.col(colsrc) < f.col(coldst), f.col(colsrc)).otherwise(f.col(coldst)),
    )
    df = df.withColumn(
        "newdst",
        when(f.col(coldst) > f.col(colsrc), f.col(coldst)).otherwise(f.col(colsrc)),
    )
    df = (
        df.drop(colsrc)
        .withColumn(colsrc, f.col("newsrc"))
        .drop(coldst)
        .withColumn(coldst, f.col("newdst"))
        .drop("newsrc", "newdst")
    )
    return df


def nodes_from_edge_df(df, src="src", dst="dst", component="component"):

    out = df.groupby(component).agg(
        f.collect_set(src).alias("_1"), f.collect_list(dst).alias("_2")
    )
    out = out.withColumn("nodes", f.array_union(f.col("_1"), f.col("_2"))).drop(
        "_1", "_2"
    )
    return out


def subgraph_stats(df, component="component", weight="weight", src="src", dst="dst"):

    """
    
input spark dataframe:

---+---+------+----------+---------------------+
|src|dst|weight| component|            distance|
+---+---+------+----------+--------------------+
|  f|  d|  0.67|         0| 0.32999999999999996|
|  f|  g|  0.34|         0|  0.6599999999999999|
|  b|  c|  0.56|8589934592| 0.43999999999999995|
|  g|  h|  0.99|         0|0.010000000000000009|
|  a|  b|   0.4|8589934592|                 0.6|
|  h|  i|   0.5|         0|                 0.5|
|  h|  j|   0.8|         0| 0.19999999999999996|
|  d|  e|  0.84|         0| 0.16000000000000003|
|  e|  f|  0.65|         0|                0.35|
+---+---+------+----------+--------------------+
    
    
output spark dataframe:

+----------+--------------------+---------+---------+------------------+
| component|               nodes|nodecount|edgecount|           density|
+----------+--------------------+---------+---------+------------------+
|8589934592|           [b, a, c]|        3|        2|0.6666666666666666|
|         0|[h, g, f, e, d, i...|        7|        7|0.3333333333333333|
+----------+--------------------+---------+---------+------------------+


    
    
    """

    edgec = df.groupby(component).agg(f.count(weight).alias("edgecount"))
    srcdf = df.groupby(component).agg(f.collect_set(src).alias("sources"))
    dstdf = df.groupby(component).agg(f.collect_set(dst).alias("destinations"))
    allnodes = srcdf.join(dstdf, on="component")
    allnodes = allnodes.withColumn(
        "nodes", f.array_union(f.col("sources"), f.col("destinations"))
    ).withColumn("nodecount", f.size(f.col("nodes")))

    output = allnodes.join(edgec, on="component")

    # density related calcs based on nodecount and max possible number of edges in an undirected graph

    output = output.withColumn(
        "maxNumberOfEdgesundir", f.col("nodecount") * (f.col("nodecount") - 1.0) / 2.0,
    )
    output = output.withColumn(
        "density", f.round(f.col("edgecount") / f.col("maxNumberOfEdgesundir"), 3)
    ).drop("sources", "destinations", "maxNumberOfEdgesundir")

    return output


def _from_unweighted_graphframe_to_nxGraph(g):
    """Takes as input:
       
           an unweighted Graphframe graph g 
           
       Returns: 
       
           an unweighted networkx graph"""

    nxGraph = nx.Graph()
    nxGraph.add_nodes_from(g.vertices.rdd.map(lambda x: x.id).collect())
    nxGraph.add_edges_from(g.edges.rdd.map(lambda x: (x.src, x.dst)).collect())
    return nxGraph


def _from_weighted_graphframe_to_nxGraph(g):
    """Takes as input:
       
           a weighted Graphframe graph g 
           (note: edge weight column needs to be called as weight on the Graphframe)
           
       Returns: 
       
           a weighted networkx graph"""

    nxGraph = nx.Graph()
    nxGraph.add_nodes_from(g.vertices.rdd.map(lambda x: x.id).collect())
    nxGraph.add_weighted_edges_from(
        g.edges.rdd.map(lambda x: (x.src, x.dst, x.weight)).collect()
    )
    return nxGraph
