import pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    StringType,
    LongType,
    ArrayType,
)
from node2vec import Node2Vec
import networkx as nx
import ast
import numpy as np
import pandas as pd


def _node2vec_embedding(
    sparkdf,
    src="src",
    dst="dst",
    cluster_id_colname="cluster_id",
    dimensions=64,
    walk_length=8,
):
    """provide node2vec embedding of each subgraph/cluster
    
    
    Args:
        sparkdf: imput edgelist Spark DataFrame
        src: src column name
        dst: dst column name
        cluster_id_colname: Graphframes connected components created cluster_id
        dimensions (int): node2vec parameter 
        walk_length (int): node2vec parameter 
        num_walks (int):node2vec parameter 
    
    Returns:
        cluster_id: Graphframes connected components created cluster_id
        n2vembed: node2vec embedded array casted as string 
        
        
    Note: in order for the array to be used it needs to be casted back to a numpy array downstream
    like this: `n2varray= np.array(ast.literal_eval(n2varraystring))`
    This has been implemented like this because of limitations on return types of PANDAS_UDFs)
        
    """

    psrc = src
    pdst = dst
    pdimensions = dimensions
    pwalk_length = walk_length

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("n2vembed", StringType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def n2v(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)
        embeddings = []
        node2vec = Node2Vec(
            nxGraph,
            dimensions=pdimensions,
            walk_length=pwalk_length,
            num_walks=200,
            workers=4,
        )

        model = node2vec.fit(window=10, min_count=1, batch_words=4)
        # Embed nodes from a nxGraph
        graph_nodes_emb_model = model.wv

        for n in nxGraph.nodes:
            embeddings.append(list(graph_nodes_emb_model.get_vector(n)))

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [str(embeddings)]], columns=["cluster_id", "n2vembed",]
        )

    out = sparkdf.groupby(cluster_id_colname).apply(n2v)

    return out
