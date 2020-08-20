import pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f

import graphframes
import networkx as nx


def graphdecompose(g):
    """Takes as input a Graphframe graph g and returns a list of connected component graphs to iterate from."""

    cc = g.connectedComponents()
    components = cc.select("component").distinct().rdd.flatMap(lambda x: x).collect()
    list_of_graphs = [cc.where(cc["component"] == c) for c in components]
    return list_of_graphs


def graphdegreecounts(g):
    """Takes as input a Graphframe graph g and returns a spark datafrane with degrees of nodes and the count of those degrees.
    Useful in order to quickly understand how connected a graph is
    """

    count = (
        g.inDegrees.selectExpr("id as id", "inDegree as degree")
        .groupBy("degree")
        .count()
    )
    return count


def graphdensity(g, directed=False):
    """Takes as input :
        a Graphframe graph g and a boolean variable directed that signifies if the graph is directed or undirected
        and returns:
        float number with the density of g
        Density is calculated according to the graph theory definition of graph density (eg.  https://en.wikipedia.org/wiki/Dense_graph )
        Useful in order to quickly understand how dense a graph is.
        Can be either used on a graph that is not decomposed to connected components (but that will be a disconeected graph and it perhaps the result will not make sense)
        It is more useful when it is iterated on each subgraph of decomposed connected components.
        """

    V = g.vertices.count()

    # max number of edges in a directed graph
    maxNumberOfEdgesdir = V * (V - 1)

    # max number of edges in a directed graph
    maxNumberOfEdgesundir = V * (V - 1) / 2

    E = g.edges.count()

    if directed == False:
        den = E / maxNumberOfEdgesundir
    else:
        den = E / maxNumberOfEdgesdir

    return den


def subgraph_stats(g, mysparksession=spark):

    """Takes as input a Graphframe graph g which is not yet decomposed 
    and returns a spark datafrane with the connected component id , the density of the subgraph and the size of the subgraph.
      
    """

    subgraph_stats_schema = StructType(
        [
            StructField("component", StringType()),
            StructField("density", FloatType()),
            StructField("graphsize", IntegerType()),
        ]
    )
    cplist = []
    denlist = []
    sizelist = []

    list_of_subgraphs = graphdecompose(g)

    for i in list_of_subgraphs:

        _id = i.select("id").rdd.map(lambda a: a.id)
        ids = _id.collect()

        current_component = (
            i.select("component").distinct().rdd.flatMap(lambda x: x).collect()
        )

        # create a subgraph for each component

        _edges = g.edges.rdd.filter(lambda a: a.dst in ids or a.src in ids).toDF()
        _vertices = g.vertices.rdd.filter(lambda a: a.id in ids).toDF()
        g_sub = GraphFrame(_vertices, _edges)

        # calculate some graph stats for each component/subgraph

        d_sub = graphdensity(g_sub)
        count_sub = g_sub.vertices.count()

        # append lists with id and stats from each component
        cplist.append(current_component[0])
        denlist.append(d_sub)
        sizelist.append(count_sub)

    graphstats_df = mysparksession.createDataFrame(
        zip(cplist, denlist, sizelist), subgraph_stats_schema
    )

    return graphstats_df
