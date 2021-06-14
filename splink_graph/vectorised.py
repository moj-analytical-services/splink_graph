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
from networkx.algorithms.distance_measures import diameter, radius
from networkx.algorithms.cluster import transitivity
from networkx.algorithms.centrality import edge_betweenness_centrality
from networkx.algorithms.bridges import bridges
from networkx.algorithms.centrality import (
    eigenvector_centrality,
    harmonic_centrality,
)
from networkx.algorithms.graph_hashing import weisfeiler_lehman_graph_hash
import os
import pandas as pd
import numpy as np

# setup to work around with pandas udf
# see answers on
# https://stackoverflow.com/questions/58458415/pandas-scalar-udf-failing-illegalargumentexception
os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

eboutSchema = StructType(
    [
        StructField("src", StringType()),
        StructField("dst", StringType()),
        StructField("eb", FloatType()),
        StructField("component", LongType()),
    ]
)


def edgebetweeness(sparkdf, src="src", dst="dst", distance="distance",component="component"):
    from pyspark.context import SparkContext, SparkConf
    from pyspark.sql import SparkSession

    conf = SparkConf()
    conf.set("spark.sql.execution.arrow.enabled", "true")
    conf.set("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
    sc = SparkContext.getOrCreate(conf=conf)

    psrc = src
    pdst = dst
    pdistance = distance

    @pandas_udf(eboutSchema, PandasUDFType.GROUPED_MAP)
    def ebdf(pdf):

        srclist = []
        dstlist = []
        eblist = []
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        eb = edge_betweenness_centrality(nxGraph, normalized=True, weight=distance)
        currentcomp = pdf[component].iloc[0]  # access current component
        compsize = pdf[component].size  # how many nodes does this cluster have?

        for srcdst, v in eb.items():
            # unpack (src,dst) tuple key
            src, dst = srcdst

            srclist.append(src)
            dstlist.append(dst)
            eblist.append(v)

        return pd.DataFrame(
            zip(srclist, dstlist, eblist, [currentcomp] * compsize),
            columns=["src", "dst", "eb", "component"],
        )

    out = sparkdf.groupby(component).apply(ebdf)
    return out


def bridge_edges(sparkdf, src="src", dst="dst", distance="distance",component="component"):

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

+---+---+------+----------+--------------------+
|src|dst|weight| component|            distance|
+---+---+------+----------+--------------------+
|  b|  c|  0.56|8589934592| 0.43999999999999995|
|  f|  g|  0.34|         0|  0.6599999999999999|
|  g|  h|  0.99|         0|0.010000000000000009|
|  h|  i|   0.5|         0|                 0.5|
|  h|  j|   0.8|         0| 0.19999999999999996|
+---+---+------+----------+--------------------+


    """
    psrc = src
    pdst = dst
    pdistance = distance

    bridgesoutSchema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("weight", DoubleType()),
            StructField("component", LongType()),
            StructField("distance", DoubleType()),
        ]
    )

    @pandas_udf(bridgesoutSchema, PandasUDFType.GROUPED_MAP)
    def br_p_udf(pdf):

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)

        b = bridges(nxGraph)
        bpdf = pd.DataFrame(b, columns=["src", "dst",])

        return pd.merge(bpdf, pdf, how="inner", on=["src", "dst"])

    out = sparkdf.groupby(component).apply(br_p_udf)
    return out


def eigencentrality(sparkdf, src="src", dst="dst", distance="distance",component="component"):

    """
    
Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.
Eigenvector Centrality was proposed by Phillip Bonacich, in his 1986 paper Power and Centrality: 
A Family of Measures. 
It was the first of the centrality measures that considered the transitive importance of a node in a graph, 
rather than only considering its direct importance. 


Relationships to high-scoring nodes contribute more to the score of a node than connections to low-scoring nodes. 
A high score means that a node is connected to other nodes that have high scores. 

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
    

+----+-------------------+
|node|   eigen_centrality|
+----+-------------------+
|   b|  0.707106690085642|
|   c| 0.5000000644180599|
|   a| 0.5000000644180599|
|   f| 0.5746147732828122|
|   d| 0.4584903903420785|
|   g|0.37778352393858183|
|   h|0.27663243805676946|
|   i|0.12277029263709134|
|   j|0.12277029263709134|
|   e| 0.4584903903420785|
+----+-------------------+
   

    """
    ecschema = StructType(
        [
            StructField("node", StringType()),
            StructField("eigen_centrality", DoubleType()),
        ]
    )

    psrc = src
    pdst = dst
    pdistance = distance

    @pandas_udf(ecschema, PandasUDFType.GROUPED_MAP)
    def eigenc(pdf):
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        ec = eigenvector_centrality(nxGraph)
        return (
            pd.DataFrame.from_dict(ec, orient="index", columns=["eigen_centrality"])
            .reset_index()
            .rename(columns={"index": "node", "eigen_centrality": "eigen_centrality"})
        )

    out = sparkdf.groupby(component).apply(eigenc)
    return out


def harmoniccentrality(sparkdf, src="src", dst="dst", distance="distance",component="component"):

    """

Harmonic centrality (also known as valued centrality) is a variant of closeness centrality, that was invented 
to solve the problem the original formula had when dealing with unconnected graphs.
Harmonic centrality was proposed by Marchiori and Latora  while trying to come up with a sensible notion of "average shortest path".
They suggested a different way of calculating the average distance to that used in the Closeness Centrality algorithm. 
Rather than summing the distances of a node to all other nodes, the harmonic centrality algorithm sums the inverse of those distances. 
This enables it deal with infinite values.


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
 
+----+-------------------+
|node|harmonic_centrality|
+----+-------------------+
|   b|                2.0|
|   c|                1.5|
|   a|                1.5|
|   f|  4.166666666666667|
|   d| 3.3333333333333335|
|   g|                4.0|
|   h|  4.166666666666667|
|   i| 2.8333333333333335|
|   j| 2.8333333333333335|
|   e| 3.3333333333333335|
+----+-------------------+
    """

    hcschema = StructType(
        [
            StructField("node", StringType()),
            StructField("harmonic_centrality", DoubleType()),
        ]
    )

    psrc = src
    pdst = dst
    pdistance = distance

    @pandas_udf(hcschema, PandasUDFType.GROUPED_MAP)
    def harmc(pdf):
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        hc = harmonic_centrality(nxGraph)
        return (
            pd.DataFrame.from_dict(hc, orient="index", columns=["harmonic_centrality"])
            .reset_index()
            .rename(
                columns={"index": "node", "harmonic_centrality": "harmonic_centrality",}
            )
        )

    out = sparkdf.groupby(component).apply(harmc)
    return out


def diameter_radius_transitivity(sparkdf, src="src", dst="dst",component="component"):
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

    @pandas_udf(
        StructType(
            [
                StructField("component", LongType()),
                StructField("diameter", IntegerType()),
                StructField("radius", IntegerType()),
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
        r = radius(nxGraph)
        t = transitivity(nxGraph)
        tric = nx.average_clustering(nxGraph)
        sq = nx.square_clustering(nxGraph)
        sqc = sum(sq.values()) / len(sq.values())
        h = weisfeiler_lehman_graph_hash(nxGraph)

        co = pdf[component].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [d] + [r] + [t] + [tric] + [sqc] + [h]],
            columns=[
                "component",
                "diameter",
                "radius",
                "transitivity",
                "tri_clustcoeff",
                "sq_clustcoeff",
                "graphhash",
            ],
        )

    out = sparkdf.groupby(component).apply(drt)
    out = (
        out.withColumn("tri_clustcoeff", f.round(f.col("tri_clustcoeff"), 3))
        .withColumn("sq_clustcoeff", f.round(f.col("sq_clustcoeff"), 3))
        .withColumn("transitivity", f.round(f.col("transitivity"), 3))
    )
    return out
