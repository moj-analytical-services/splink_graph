import pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import StructField,StructType,FloatType,StringType,LongType
from node2vec import Node2Vec
import networkx as nx


def node2vec_embedding(nxGraph,dimensions=64, walk_length=8):
    node2vec = Node2Vec(nxGraph, dimensions=dimensions, walk_length=walk_length, num_walks=200, workers=4)

    model = node2vec.fit(window=10, min_count=1, batch_words=4)  
    # Embed nodes from one nxGraph
    graph_nodes_emb = model.wv
    
    return graph_nodes_emb
