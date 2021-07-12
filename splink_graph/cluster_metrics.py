import pyspark
from pyspark.sql.types import (
    LongType,
    StringType,
    FloatType,
    IntegerType,
    DoubleType,
    StructType,
    StructField,
)
import pyspark.sql.functions as f
from pyspark.sql.functions import pandas_udf, PandasUDFType
import networkx as nx
import networkx.algorithms.community as nx_comm
from networkx.algorithms.distance_measures import diameter, radius
from networkx.algorithms.cluster import transitivity
from networkx.algorithms.centrality import edge_betweenness_centrality
from networkx.algorithms.bridges import bridges
from networkx.algorithms.centrality import (
    eigenvector_centrality,
    harmonic_centrality,
)
from networkx.algorithms.community.centrality import girvan_newman
from networkx.algorithms.graph_hashing import weisfeiler_lehman_graph_hash
import os
import pandas as pd
import numpy as np

# setup to work around with pandas udf
# see answers on
# https://stackoverflow.com/questions/58458415/pandas-scalar-udf-failing-illegalargumentexception


def cluster_main_stats(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id"):
    """    

    input spark dataframe:

---+---+------+----------+---------------------+
|src|dst|weight|cluster_id|            distance|
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




    """

    psrc = src
    pdst = dst

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("diameter", IntegerType()),
                StructField("transitivity", FloatType()),
                StructField("tri_clustcoeff", FloatType()),
                StructField("sq_clustcoeff", FloatType()),
                StructField("graphhash", StringType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def drt(pdf):

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)
        d = diameter(nxGraph)
        t = transitivity(nxGraph)
        tric = nx.average_clustering(nxGraph)
        sq = nx.square_clustering(nxGraph)
        sqc = sum(sq.values()) / len(sq.values())
        h = weisfeiler_lehman_graph_hash(nxGraph)

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [d] + [t] + [tric] + [sqc] + [h]],
            columns=[
                "cluster_id",
                "diameter",
                "transitivity",
                "tri_clustcoeff",
                "sq_clustcoeff",
                "graphhash",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(drt)
    
    
    
    out = (
        out.withColumn("tri_clustcoeff", f.round(f.col("tri_clustcoeff"), 3))
        .withColumn("sq_clustcoeff", f.round(f.col("sq_clustcoeff"), 3))
        .withColumn("transitivity", f.round(f.col("transitivity"), 3))
    )
    return out


def cluster_connectivity(
    sparkdf, src="src", dst="dst", distance="distance", cluster_id_colname="cluster_id"
):
    """    
    Measures the minimal number of vertices that can be removed to disconnect the graph.
    Larger vertex (node) connectivity --> harder to disconnect graph
    
    Measures the minimal number of edges that can be removed to disconnect the graph.
    Larger edge connectivity --> harder to disconnect graph
    
    The global efficiency of a graph is the average inverse distance between all pairs of nodes in the graph.
    The larger the average inverse shortest path distance, the more robust the graph.
    This can be viewed through the lens of network connectivity i.e., larger average inverse distance
    --> better connected graph --> more robust graph
    

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

    """

    psrc = src
    pdst = dst
    pdistance = distance

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("node_conn", IntegerType()),
                StructField("edge_conn", IntegerType()),
                StructField("global_efficiency", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def conn_eff(pdf):

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)

        nc = nx.algorithms.node_connectivity(nxGraph)
        ec = nx.algorithms.edge_connectivity(nxGraph)
        ge = round(nx.global_efficiency(nxGraph), 3)

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [nc] + [ec] + [ge]],
            columns=["cluster_id", "node_conn", "edge_conn", "global_efficiency"],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(conn_eff)

    return out


def cluster_modularity(
    sparkdf, src="src", dst="dst", distance="distance", cluster_id_colname="cluster_id"
):
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

    """

    psrc = src
    pdst = dst
    pdistance = distance

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("comp_eb_modularity", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def cluster_eb_modularity(pdf):

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)

        ## TODO: comments! to document this code

        def most_central_edge(G):
            centrality = edge_betweenness_centrality(
                G, weight=pdistance, normalized=True
            )
            return max(centrality, key=centrality.get)

        comp = girvan_newman(nxGraph, most_valuable_edge=most_central_edge)
        gn = tuple(sorted(c) for c in next(comp))

        co = pdf[cluster_id].iloc[0]  # access component id
        co_eb_mod = nx_comm.modularity(nxGraph, gn)

        return pd.DataFrame(
            [[co] + [co_eb_mod]], columns=["cluster_id", "cluster_eb_modularity",],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(cluster_eb_modularity)

    return out


def cluster_avg_edge_betweenness(
    sparkdf, src="src", dst="dst", distance="distance", cluster_id_colname="cluster_id"
):
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

    """

    psrc = src
    pdst = dst
    pdistance = distance

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("avg_cluster_eb", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def avg_eb(pdf):

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        edge_btwn = edge_betweenness_centrality(
            nxGraph, normalized=True
        )

        if len(edge_btwn) > 0:
            aeb = round(sum(list(edge_btwn.values())) / len(edge_btwn), 3)
        else:
            aeb = 0.0

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame([[co] + [aeb]], columns=["cluster_id", "avg_cluster_eb",],)

    out = sparkdf.groupby(cluster_id_colname).apply(avg_eb)

    return out
