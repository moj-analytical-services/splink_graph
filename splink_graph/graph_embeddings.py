import networkx as nx
from node2vec import Node2Vec
from node2vec.edges import HadamardEmbedder

def node2vec_nx_edge_embeddings(nxGraph):

    node2vec = Node2Vec(nxGraph, dimensions=64, walk_length=30, num_walks=200, workers=4)  # Use temp_folder for big graphs

    # Embed nodes
    # Any keywords acceptable by gensim.Word2Vec can be passed, `dimensions` and `workers` are automatically passed (from the Node2Vec constructor)
    # batch_words here are node-sequences
    model = node2vec.fit(window=10, min_count=1, batch_words=4)  


    # Embed nodes 

    nodes_embs = model.wv
    
    # Embed edges using Hadamard method
    
    edges_embs = HadamardEmbedder(keyed_vectors=model.wv)
    
    


