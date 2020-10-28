import networkx as nx
from networkx import *
from networkx.algorithms import *


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
