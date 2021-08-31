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
    sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id",
):

    """
    Args:
        sparkdf: imput edgelist Spark DataFrame
        src: src column name
        dst: dst column name
        distance_colname: distance column name
        cluster_id_colname: Graphframes-created connected components created cluster_id

    Returns:
        node_id:
        eigen_centrality: eigenvector centrality of cluster cluster_id
        cluster_id: cluster_id corresponding to the node_id

Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.
Eigenvector Centrality was proposed by Phillip Bonacich, in his 1986 paper Power and Centrality:
A Family of Measures.
It was the first of the centrality measures that considered the transitive importance of a node in a graph,
rather than only considering its direct importance.
Relationships to high-scoring nodes contribute more to the score of a node than connections to low-scoring nodes.
A high score means that a node is connected to other nodes that have high scores.

example input spark dataframe


|src|dst|weight|cluster_id|distance|
|---|---|------|----------|--------|
|  f|  d|  0.67|         0|   0.329|
|  f|  g|  0.34|         0|   0.659|
|  b|  c|  0.56|8589934592|   0.439|
|  g|  h|  0.99|         0|   0.010|
|  a|  b|   0.4|8589934592|     0.6|
|  h|  i|   0.5|         0|     0.5|
|  h|  j|   0.8|         0|   0.199|
|  d|  e|  0.84|         0|   0.160|
|  e|  f|  0.65|         0|    0.35|


example output spark dataframe


|node_id|   eigen_centrality|cluster_id|
|-------|-------------------|----------|
|   b   |  0.707106690085642|8589934592|
|   c   | 0.5000000644180599|8589934592|
|   a   | 0.5000000644180599|8589934592|
|   f   | 0.5746147732828122|         0|
|   d   | 0.4584903903420785|         0|
|   g   |0.37778352393858183|         0|
|   h   |0.27663243805676946|         0|
|   i   |0.12277029263709134|         0|
|   j   |0.12277029263709134|         0|
|   e   | 0.4584903903420785|         0|



    """
    ecschema = StructType(
        [
            StructField("node_id", StringType()),
            StructField("eigen_centrality", DoubleType()),
            StructField(cluster_id_colname, LongType()),
        ]
    )

    psrc = src
    pdst = dst

    @pandas_udf(ecschema, PandasUDFType.GROUPED_MAP)
    def eigenc(pdf: pd.DataFrame) -> pd.DataFrame:
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)
        ec = eigenvector_centrality(nxGraph, tol=1e-03)
        out_df = (
            pd.DataFrame.from_dict(ec, orient="index", columns=["eigen_centrality"])
            .reset_index()
            .rename(
                columns={"index": "node_id", "eigen_centrality": "eigen_centrality"}
            )
        )

        cluster_id = pdf[cluster_id_colname][0]
        out_df[cluster_id_colname] = cluster_id
        return out_df

    out = sparkdf.groupby(cluster_id_colname).apply(eigenc)
    return out


def harmoniccentrality(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id"):

    """
    Args:
        sparkdf: imput edgelist Spark DataFrame
        src: src column name
        dst: dst column name
        distance_colname: distance column name
        cluster_id_colname: Graphframes-created connected components created cluster_id

    Returns:
        node_id:
        harmonic_centrality: Harmonic centrality of cluster cluster_id
        cluster_id: cluster_id corresponding to the node_id

Harmonic centrality (also known as valued centrality) is a variant of closeness centrality, that was invented
to solve the problem the original formula had when dealing with unconnected graphs.
Harmonic centrality was proposed by Marchiori and Latora  while trying to come up with a sensible notion of "average shortest path".
They suggested a different way of calculating the average distance to that used in the Closeness Centrality algorithm.
Rather than summing the distances of a node to all other nodes, the harmonic centrality algorithm sums the inverse of those distances.
This enables it deal with infinite values.


input spark dataframe:

|src|dst|weight|cluster_id|distance|
|---|---|------|----------|--------|
|  f|  d|  0.67|         0|   0.329|
|  f|  g|  0.34|         0|   0.659|
|  b|  c|  0.56|8589934592|   0.439|
|  g|  h|  0.99|         0|   0.010|
|  a|  b|   0.4|8589934592|     0.6|
|  h|  i|   0.5|         0|     0.5|
|  h|  j|   0.8|         0|   0.199|
|  d|  e|  0.84|         0|   0.160|
|  e|  f|  0.65|         0|    0.35|

output spark dataframe:


|node_id|harmonic_centrality|cluster_id|
|-------|-------------------|----------|
|   b   |                2.0|8589934592|
|   c   |                1.5|8589934592|
|   a   |                1.5|8589934592|
|   f   |  4.166666666666667|         0|
|   d   | 3.3333333333333335|         0|
|   g   |                4.0|         0|
|   h   |  4.166666666666667|         0|
|   i   | 2.8333333333333335|         0|
|   j   | 2.8333333333333335|         0|
|   e   | 3.3333333333333335|         0|

    """

    hcschema = StructType(
        [
            StructField("node_id", StringType()),
            StructField("harmonic_centrality", DoubleType()),
            StructField(cluster_id_colname, LongType()),
        ]
    )

    psrc = src
    pdst = dst

    @pandas_udf(hcschema, PandasUDFType.GROUPED_MAP)
    def harmc(pdf: pd.DataFrame) -> pd.DataFrame:
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)
        hc = harmonic_centrality(nxGraph)
        out_df = (
            pd.DataFrame.from_dict(hc, orient="index", columns=["harmonic_centrality"])
            .reset_index()
            .rename(
                columns={
                    "index": "node_id",
                    "harmonic_centrality": "harmonic_centrality",
                }
            )
        )
        cluster_id = pdf[cluster_id_colname][0]
        out_df[cluster_id_colname] = cluster_id
        return out_df

    out = sparkdf.groupby(cluster_id_colname).apply(harmc)
    return out
