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
from networkx.algorithms.bridges import bridges
from networkx.algorithms.centrality import edge_betweenness_centrality


def edgebetweeness(
    sparkdf,
    src="src",
    dst="dst",
    distance_col="distance",
    cluster_id_colname="cluster_id",
):
    """return edge betweenness
    
    Args:
        sparkdf: imput edgelist Spark DataFrame
        src: src column name
        dst: dst column name
        distance_colname: distance column name
        cluster_id_colname: Graphframes-created connected components created cluster_id
    

    
    """

    psrc = src
    pdst = dst
    pdistance = distance_col

    eboutSchema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("eb", FloatType()),
            StructField("cluster_id", LongType()),
        ]
    )

    @pandas_udf(eboutSchema, PandasUDFType.GROUPED_MAP)
    def ebdf(pdf: pd.DataFrame) -> pd.DataFrame:

        srclist = []
        dstlist = []
        eblist = []
        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        eb = edge_betweenness_centrality(nxGraph, normalized=True, weight=pdistance)
        currentcomp = pdf[cluster_id_colname].iloc[0]  # access current component
        compsize = pdf[
            cluster_id_colname
        ].size  # how many nodes does this cluster have?

        for srcdst, v in eb.items():
            # unpack (src,dst) tuple key
            src, dst = srcdst

            srclist.append(src)
            dstlist.append(dst)
            eblist.append(round(v, 3))

        return pd.DataFrame(
            zip(srclist, dstlist, eblist, [currentcomp] * compsize),
            columns=["src", "dst", "eb", "cluster_id"],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(ebdf)
    return out


def bridge_edges(
    sparkdf,
    src="src",
    dst="dst",
    weight_colname="weight",
    distance_colname="distance",
    cluster_id_colname="cluster_id",
):

    """return edges that are bridges
    
    Args:
        sparkdf: imput edgelist Spark DataFrame
        src: src column name
        dst: dst column name
        distance_colname: distance column name
        cluster_id_colname: Graphframes-created connected components created cluster_id
        
    Returns:    
            src: 
            dst:
            distance:
            weight:
            cluster_id: Graphframes-created connected components created cluster_id


example input spark dataframe

|src|dst|weight|cluster_id|distance|
+---|---|------|----------|--------|
|  f|  d|  0.67|         0|0.329   |
|  f|  g|  0.34|         0|0.659   |
|  b|  c|  0.56|8589934592| 0.439  |
|  g|  h|  0.99|         0|0.010   |
|  a|  b|   0.4|8589934592|0.6     |
|  h|  i|   0.5|         0|0.5     |
|  h|  j|   0.8|         0| 0.199  |
|  d|  e|  0.84|         0| 0.160  |
|  e|  f|  0.65|         0|0.35    |

    
example output spark dataframe


|src|dst|weight|cluster_id|distance|
+---|---|------|----------|--------|
|  b|  c|  0.56|8589934592|0.439   |
|  f|  g|  0.34|         0|0.659   |
|  g|  h|  0.99|         0|0.010   |
|  h|  i|   0.5|         0|0.5     |
|  h|  j|   0.8|         0|0.199   |


    """
    psrc = src
    pdst = dst
    pweight = weight_colname
    pdistance = distance_colname
    pcomponent = cluster_id_colname

    bridgesoutSchema = StructType(
        [
            StructField(psrc, StringType()),
            StructField(pdst, StringType()),
            StructField(pweight, DoubleType()),
            StructField(pcomponent, LongType()),
            StructField(pdistance, DoubleType()),
        ]
    )

    @pandas_udf(bridgesoutSchema, PandasUDFType.GROUPED_MAP)
    def br_p_udf(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)

        b = bridges(nxGraph)
        bpdf = pd.DataFrame(b, columns=[psrc, pdst])

        return pd.merge(bpdf, pdf, how="inner", on=[psrc, pdst])

    indf = sparkdf.select(psrc, pdst, pweight, pdistance, pcomponent)
    out = indf.groupby(cluster_id_colname).apply(br_p_udf)
    return out
