import pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import StructField,StructType,FloatType,StringType,LongType
from node2vec import Node2Vec
import networkx as nx
import ast




def _node2vec_embedding(nxGraph,dimensions=64, walk_length=8):
    
    embeddings=[]
    node2vec = Node2Vec(nxGraph, dimensions=dimensions, walk_length=walk_length, num_walks=200, workers=4)

    model = node2vec.fit(window=10, min_count=1, batch_words=4)  
    # Embed nodes from a nxGraph
    graph_nodes_emb_model = model.wv
    
    
    for n in nxGraph.nodes:
        embeddings.append(list(graph_nodes_emb_model.get_vector(n)))
    
    return graph_nodes_emb
