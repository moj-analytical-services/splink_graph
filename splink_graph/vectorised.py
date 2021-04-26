import pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f
from graphframes import *


import networkx as nx
from networkx import *
from networkx.algorithms import *
from networkx.algorithms.distance_measures import diameter


from pyspark.sql.functions import pandas_udf, PandasUDFType, sum, max, col, concat, lit
import sys
import os

# setup to work around with pandas udf
# see answers here https://stackoverflow.com/questions/58458415/pandas-scalar-udf-failing-illegalargumentexception
os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

import pandas as pd
import numpy as np


eboutSchema = StructType(
    [
        StructField("src", StringType()),
        StructField("dst", StringType()),
        StructField("eb", FloatType()),
    ]
)


@pandas_udf(eboutSchema, PandasUDFType.GROUPED_MAP)
def ebdf(pdf, src="src", dst="dst", distance="distance"):
    """

Takes as input :
        an edges spark dataframe (can be describing a disconnected graph or a connected one)
    Returns:
        a spark dataframe consiting of [src,dst, edgebetweeness] rows  
        
        
    Betweenness of an edge e is the sum of the fraction of all-pairs shortest paths that pass through e
    It is very similar metric to articulation but it works on edges instead of nodes.
    
    An edge with a high edge betweenness score represents a bridge-like connector between two parts of a graph, 
    the removal of which may affect the communication between many pairs of nodes/vertices through the shortest paths between them.

input spark dataframe:

---+---+------+-----------+--------------------+
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
    
+---+---+----------+
|src|dst|        eb|
+---+---+----------+
|  b|  c| 0.6666667|
|  b|  a| 0.6666667|
|  f|  d|0.23809524|
|  f|  g| 0.5714286|
|  f|  e|0.23809524|
|  d|  e|0.04761905|
|  g|  h| 0.5714286|
|  h|  i| 0.2857143|
|  h|  j| 0.2857143|
+---+---+----------+
    
    """

    srclist = []
    dstlist = []
    eblist = []
    nxGraph = nx.Graph()
    nxGraph = nx.from_pandas_edgelist(pdf, src, dst, distance)
    eb = nx.edge_betweenness_centrality(nxGraph, normalized=True, weight=distance)
    for srcdst, v in eb.items():
        # unpack (src,dst) tuple key
        src, dst = srcdst

        srclist.append(src)
        dstlist.append(dst)
        eblist.append(v)

    return pd.DataFrame(zip(srclist, dstlist, eblist), columns=["src", "dst", "eb"])


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
def bridges(pdf):

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

    nxGraph = nx.Graph()
    nxGraph = nx.from_pandas_edgelist(pdf, "src", "dst", "distance")

    b = nx.bridges(nxGraph)
    bpdf = pd.DataFrame(b, columns=["src", "dst",])

    return pd.merge(bpdf, pdf, how="inner", on=["src", "dst"])


ecschema = StructType(
    [StructField("node", StringType()), StructField("eigen_centrality", DoubleType()),]
)


@pandas_udf(ecschema, PandasUDFType.GROUPED_MAP)
def eigencentrality(pdf):
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

    nxGraph = nx.Graph()
    nxGraph = nx.from_pandas_edgelist(pdf, "src", "dst", "distance")
    ec = eigenvector_centrality(nxGraph)
    return (
        pd.DataFrame.from_dict(ec, orient="index", columns=["eigen_centrality"])
        .reset_index()
        .rename(columns={"index": "node", "eigen_centrality": "eigen_centrality"})
    )


hcschema = StructType(
    [
        StructField("node", StringType()),
        StructField("harmonic_centrality", DoubleType()),
    ]
)


@pandas_udf(hcschema, PandasUDFType.GROUPED_MAP)
def harmoniccentrality(pdf):

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

    nxGraph = nx.Graph()
    nxGraph = nx.from_pandas_edgelist(pdf, "src", "dst", "distance")
    hc = harmonic_centrality(nxGraph)
    return (
        pd.DataFrame.from_dict(hc, orient="index", columns=["harmonic_centrality"])
        .reset_index()
        .rename(columns={"index": "node", "harmonic_centrality": "harmonic_centrality"})
    )


tvschema = StructType(
    [StructField("component", LongType()), StructField("transitivity", FloatType())]
)


@pandas_udf(tvschema, functionType=PandasUDFType.GROUPED_MAP)
def transitivity(pdf):

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
    
+----------+------------+
| component|transitivity|
+----------+------------+
|8589934592|         0.0|
|         0|  0.33333334|
+----------+------------+ 

    
    """

    nxGraph = nx.Graph()
    nxGraph = nx.from_pandas_edgelist(pdf, "src", "dst")
    b = nx.transitivity(nxGraph)

    gr = pdf["component"].iloc[0]

    return pd.DataFrame([[gr] + [b]], columns=["component", "transitivity"])

@pandas_udf(StructType([
    
    StructField("component", LongType()),
    StructField("diameter", IntegerType())
                       
                       ]), functionType=PandasUDFType.GROUPED_MAP)


def subgraphdiameter(pdf):    
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

+----------+--------+
| component|diameter|
+----------+--------+
|8589934592|       2|
|         0|       4|
+----------+--------+

    """ 
    
    nxGraph = nx.Graph()
    nxGraph=nx.from_pandas_edgelist(pdf, "src", "dst","weight" )
    d=diameter(nxGraph)
    
    gr = pdf['component'].iloc[0]
    

        
    return  pd.DataFrame([ [gr]+[d] ],columns=["component","diameter"])
