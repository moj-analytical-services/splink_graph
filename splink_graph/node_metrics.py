import pyspark
import networkx as nx
import pandas as pd
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
from networkx.algorithms.centrality import (
    eigenvector_centrality,
    harmonic_centrality,
)


def eigencentrality(
    sparkdf, src="src", dst="dst", distance_colname="distance", cluster_id_colname="cluster_id"
):

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
            StructField("node_id", StringType()),
            StructField("eigen_centrality", DoubleType()),
        ]
    )

    psrc = src
    pdst = dst
    pdistance = distance_colname

    @pandas_udf(ecschema, PandasUDFType.GROUPED_MAP)
    def eigenc(pdf):
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        ec = eigenvector_centrality(nxGraph)
        return (
            pd.DataFrame.from_dict(ec, orient="index", columns=["eigen_centrality"])
            .reset_index()
            .rename(columns={"index": "node_id", "eigen_centrality": "eigen_centrality"})
        )

    out = sparkdf.groupby(cluster_id_colname).apply(eigenc)
    return out


def harmoniccentrality(
    sparkdf, src="src", dst="dst", distance_colname="distance", cluster_id_colname="cluster_id"
):

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
            StructField("node_id", StringType()),
            StructField("harmonic_centrality", DoubleType()),
        ]
    )

    psrc = src
    pdst = dst
    pdistance = distance_colname

    @pandas_udf(hcschema, PandasUDFType.GROUPED_MAP)
    def harmc(pdf):
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        hc = harmonic_centrality(nxGraph)
        return (
            pd.DataFrame.from_dict(hc, orient="index", columns=["harmonic_centrality"])
            .reset_index()
            .rename(
                columns={"index": "node_id", "harmonic_centrality": "harmonic_centrality",}
            )
        )

    out = sparkdf.groupby(cluster_id_colname).apply(harmc)
    return out
