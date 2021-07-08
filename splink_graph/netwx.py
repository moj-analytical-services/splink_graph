import networkx as nx
from networkx import *
from networkx.algorithms import *
from scipy import sparse
import numpy as np


def _nx_compute_all_pairs_shortest_path(nxgraph, weight=None, normalize=False):

    """
    Takes as input :
        a networkx graph nxGraph
            
    Returns: a dictionary of the computed shortest path lengths between all nodes in a graph. Accepts weighted or unweighted graphs
    
    """

    lengths = nx.all_pairs_dijkstra_path_length(nxgraph, weight=weight)
    lengths = dict(lengths)  # for compatibility with network 1.11 code
    return lengths


def _nx_longest_shortest_path(lengths):
    """
    Takes as input :
        the output of _nx_compute_all_pairs_shortest_path function which is a dictionary of shortest paths from the graph that function took as input
            
    Returns: the longest shortest path 
    This is also known as the *diameter* of a graph

    
    """

    max_length = max([max(lengths[i].values()) for i in lengths])
    return max_length


def _laplacian_matrix(nxgraph):
    """

    Takes as input :
            undirected NetworkX graph
            
    A: Adjacency Matrix
    D: Diagonal Matrix
    L: Laplacian Matrix
            
     Returns:
            Scipy sparse format Laplacian matrix
    """
    A = nx.to_scipy_sparse_matrix(
        nxgraph, format="csr", dtype=np.float, nodelist=graph.nodes
    )
    D = sparse.spdiags(
        data=A.sum(axis=1).flatten(),
        diags=[0],
        m=len(nxgraph),
        n=len(nxgraph),
        format="csr",
    )
    L = D - A

    return L


def _laplacian_spectrum(nxgraph):

    la_spectrum = nx.laplacian_spectrum(nxgraph)
    la_spectrum = np.sort(la_spectrum)  # sort ascending

    return la_spectrum
