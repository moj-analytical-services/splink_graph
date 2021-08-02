import networkx as nx
from scipy import sparse
import numpy as np
import pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.types import LongType, StringType, FloatType, DoubleType
import os


def _get_local_site_packages_dir():
    import splink_graph

    return splink_graph.__file__[0:-11]


def _create_spark_jars_string():
    sitestr = _get_local_site_packages_dir()
    if (pyspark.__version__).startswith("2"):
        spark_jars_string = (
            sitestr
            + "jars/graphframes-0.6.0-spark2.3-s_2.11.jar,"
            + sitestr
            + "jars/scala-logging-api_2.11-2.1.2.jar, "
            + sitestr
            + "jars/scala-logging-slf4j_2.11-2.1.2.jar"
        )
    else:
        spark_jars_string = sitestr + "jars/graphframes-0.8.0-spark3.0-s_2.12"
    return spark_jars_string


def _graphharmoniser(sparkdf, colsrc, coldst):

    sparkdf = sparkdf.withColumn(
        "newsrc",
        when(f.col(colsrc) < f.col(coldst), f.col(colsrc)).otherwise(f.col(coldst)),
    )
    sparkdf = sparkdf.withColumn(
        "newdst",
        when(f.col(coldst) > f.col(colsrc), f.col(coldst)).otherwise(f.col(colsrc)),
    )
    sparkdf = (
        sparkdf.drop(colsrc)
        .withColumn(colsrc, f.col("newsrc"))
        .drop(coldst)
        .withColumn(coldst, f.col("newdst"))
        .drop("newsrc", "newdst")
    )
    return sparkdf


def _probability_to_normalised_bayes_factor(
    df, prob_colname, out_colname="match_score_norm"
):

    df = df.withColumn(
        "__match_score__", f.expr(f"log2({prob_colname}/(1-{prob_colname}))")
    )

    log2_bf = f"log2({prob_colname}/(1-{prob_colname}))"
    expr = f"""
    case
        when {prob_colname} = 0.0 then -40
        when {prob_colname} = 1.0 then 40
        when {log2_bf} > 40 then 40
        when {log2_bf} < -40 then -40
        else {log2_bf}
        end
    """

    df = df.withColumn("__match_score__", f.expr(expr))

    score_min = df.select(f.min("__match_score__")).collect()[0][0]
    score_max = df.select(f.max("__match_score__")).collect()[0][0]

    expr = f"""
    1.01 - ((__match_score__ - {score_min})/{score_max - score_min} )
    """
    df = df.withColumn(out_colname, f.expr(expr))

    df = df.drop("match_score")

    return df


def _assert_columns(sparkdf, columns):
    """ 
    Raise an exception if the dataframe
    does not contain the desired columns
    
    Args:
    
        sparkdf (pyspark.sql.DataFrame): 
        columns (list of strings):  Set of columns that must be present in df
    Raises:
        ValueError: if the dataframe does not contain the desired columns
    
    
    """
    have = set(df.columns)
    need = set(columns)
    if not need.issubset(have):
        raise ValueError(
            "Missing columns in DataFrame: %s" % (", ".join(need.difference(have)))
        )


def nodes_from_edge_df(edge_df, src="src", dst="dst", id_colname="id"):

    nsrc = edge_df.select(src).withColumnRenamed(src, id_colname)
    ndst = edge_df.select(dst).withColumnRenamed(dst, id_colname)
    nodes = nsrc.union(ndst).distinct()

    return nodes


def _nodearray_from_edge_df(
    sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id"
):

    out = sparkdf.groupby(cluster_id_colname).agg(
        f.collect_set(src).alias("_1"), f.collect_list(dst).alias("_2")
    )
    out = out.withColumn("nodes", f.array_union(f.col("_1"), f.col("_2"))).drop(
        "_1", "_2"
    )
    return out


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

    Args:
        nxgraph: undirected NetworkX graph

    Returns:
        L: Scipy sparse format Laplacian matrix
            
            
            
    The Laplacian matrix L = D - A  is calculated where
    A: Adjacency Matrix
    D: Diagonal Matrix
    L: Laplacian Matrix
    """
    A = nx.to_scipy_sparse_matrix(
        nxgraph, format="csr", dtype=np.float, nodelist=nxgraph.nodes
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
    """
    Args:
        nxgraph: undirected NetworkX graph

    Returns:
       la_spectrum: laplacian spectrum of the graph
    
    """

    la_spectrum = nx.laplacian_spectrum(nxgraph)
    la_spectrum = np.sort(la_spectrum)  # sort ascending

    return la_spectrum


def _from_unweighted_graphframe_to_nxGraph(g):
    """
    
    Args:
    
        g (Graphframe): an unweighted Graphframe graph g    
           
           
    Returns: 
       
        nxGraph:    an unweighted networkx graph"""

    nxGraph = nx.Graph()
    nxGraph.add_nodes_from(g.vertices.rdd.map(lambda x: x.id).collect())
    nxGraph.add_edges_from(g.edges.rdd.map(lambda x: (x.src, x.dst)).collect())
    return nxGraph


def _from_weighted_graphframe_to_nxGraph(g):
    """
        Args:
       
           g (Graphframe): a weighted Graphframe graph g (note: edge weight column needs to be called as weight on the Graphframe)
           
        Returns: 
       
           nxGraph:a weighted networkx graph"""

    nxGraph = nx.Graph()
    nxGraph.add_nodes_from(g.vertices.rdd.map(lambda x: x.id).collect())
    nxGraph.add_weighted_edges_from(
        g.edges.rdd.map(lambda x: (x.src, x.dst, x.weight)).collect()
    )
    return nxGraph


# read a directory of edgelist files and import them into networkx graphs
def read_edgelists_from_dir(directory):
    
    listofnxgraphs = []
    if "/" in directory:
        directory = directory.strip("/")
    
    for filename in os.listdir(directory):
        if filename.endswith("edgelist"):

            current_g = nx.read_edgelist(directory + "/" + filename)
            relabeled_g = nx.convert_node_labels_to_integers(
                current_g, first_label=0, ordering="default", label_attribute="node_id"
            )

            listofnxgraphs.append(relabeled_g)

    return listofnxgraphs
