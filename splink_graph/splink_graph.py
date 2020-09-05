import pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f
from graphframes import *
import networkx as nx
from pyspark.sql.types import *
from networkx import *



def _from_graphframe_to_nxGraph(g):
       """Takes as input:
       
           a Graphframe graph g 
           
       Returns: 
       
           a networkx graph"""
        
    nxGraph = nx.Graph()
    nxGraph.add_nodes_from(g.vertices.rdd.map(lambda x: x.id).collect())
    nxGraph.add_edges_from(g.edges.rdd.map(lambda x: (x.src, x.dst)).collect())
    return nxGraph



def graphdecompose(g):
    """Takes as input:
           a Graphframe graph g (usually a disconnected undirected graph)
       Returns: 
           a list of connected component subgraphs to iterate from."""

    cc = g.connectedComponents()
    components = cc.select("component").distinct().rdd.flatMap(lambda x: x).collect()
    list_of_graphs = [cc.where(cc["component"] == c) for c in components]
    return list_of_graphs


def graphdegreecounts(g):
    """Takes as input : 
            a Graphframe graph g  
       Returns:
           a spark datafrane with degrees of nodes and the count of those degrees.
   
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
        
        Returns:
        
            float number with the density of graph g
        
        
        Density is calculated according to the graph theory definition of graph density (eg.  https://en.wikipedia.org/wiki/Dense_graph ).Useful in order to quickly understand how dense a graph is.
        
        Can be either used on a graph that is not decomposed to connected components 
        (but that will be a disconeected graph and it perhaps the result will not make sense)
        
        This function however is more useful when iterated over each subgraph of decomposed connected components.
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


def shortest_paths(g):
    
    """
    Takes as input :
        a Graphframe graph g
            
    Returns: shortest paths from each vertex to the given set of landmark vertices, 
    where landmarks are specified by vertex ID
    
    """
    
    
    v = [row.asDict()["id"] for row in g.vertices.collect()]
    return g.shortestPaths(landmarks=v)

def overall_clustering_coefficient(g):
        # dataframe containing num_triangles
        num_triangles_frame = g.triangleCount()
        
        # dataframe containing degrees
        degrees_frame = g.inDegrees
        
        
        # caculate the number of triangles, x3
        row_triangles = num_triangles_frame.agg({"count":"sum"}).collect()[0]
        num_triangles = row_triangles.asDict()['sum(count)']
        
        # calculate the number of triples
        degrees_frame = degrees_frame.withColumn("triples", 
                                                 degrees_frame.inDegree*(degrees_frame.inDegree-1)/2.0)
        row_triples = degrees_frame.agg({"triples":"sum"}).collect()[0]
        num_triples = row_triples.asDict()['sum(triples)']
        
        
        if num_triples==0:
            return 0
        else:
            return (1.0*num_triangles/num_triples)


def subgraph_stats(g, spark):

    """Takes as input:
    
          a Graphframe graph g that is disconnected (not yet decomposed) 
    
        Returns :
        
          This function iterates over each subgraph that is created by the connected components function and outputs
          a spark datafrane with the connected component id , the density of the subgraph and the size of the  subgraph.
          
        
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

    graphstats_df = spark.createDataFrame(
        zip(cplist, denlist, sizelist), subgraph_stats_schema
    )

    return graphstats_df


def articulationpoints(g, spark):
    """

    Takes as input :
        a Graphframe graph g 
    Returns:
        a spark dataframe consiting of [node id,articulation of node] rows . 
        If node is articulation point then its articulation is 1 else 0.

    A vertex/node in an undirected connected graph is an articulation point (or cut vertex) if and only if removing it (and edges through it) disconnects the graph. 
    
        For a disconnected undirected graph, an articulation point is a vertex/node which when removed increases the number of connected components.
    
    Articulation points represent vulnerabilities in a connected network – single points whose failure would split the network into 2 or more components. 


    """

    connectedCount = g.connectedComponents().select("component").distinct().count()
    vertexList = g.vertices.rdd.map(lambda x: x.id).collect()
    vertexArticulation = []

    # For each vertex, generate a new graphframe missing that vertex
    # and calculate connected component count. Then append count to
    # the output

    for vertex in vertexList:
        graphFrame = GraphFrame(
            g.vertices.filter('id != "' + vertex + '"'),
            g.edges.filter('src != "' + vertex + '"').filter('dst !="' + vertex + '"'),
        )
        count = graphFrame.connectedComponents().select("component").distinct().count()
        vertexArticulation.append((vertex, 1 if count > connectedCount else 0))

    return spark.createDataFrame(
        spark.sparkContext.parallelize(vertexArticulation), ["id", "articulation"],
    )


def edgebetweenessdf(g):
    """

    Takes as input :
        a Graphframe graph g (can be a disconnected graph or a connected one)
    Returns:
        a spark dataframe consiting of [src,dst, edgebetweeness] rows  
        
        
    Betweenness of an edge e is the sum of the fraction of all-pairs shortest paths that pass through e
    It is very similar metric to articulation but it works on edges instead of nodes.
    
    An edge with a high edge betweenness score represents a bridge-like connector between two parts of a graph, the removal of which may affect the communication between many pairs of nodes/vertices through the shortest paths between them.


    """

    srclist = []
    dstlist = []
    eblist = []
    ebSchema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("edgebetweeness", FloatType()),
        ]
    )

    # create a networkx graph from a graphframe graph

    nGraph = nx.Graph()
    nGraph.add_nodes_from(g.vertices.rdd.map(lambda x: x.id).collect())
    nGraph.add_edges_from(g.edges.rdd.map(lambda x: (x.src, x.dst)).collect())

    # use networkx eb function
    eb = nx.edge_betweenness_centrality(nGraph, normalized=True, weight=None)
    # great! but its a dict with a tuple as key (src,dst) and a value

    for srcdst, v in eb.items():

        # unpack (src,dst) tuple key
        src, dst = srcdst

        srclist.append(src)
        dstlist.append(dst)
        eblist.append(v)

    # get it back to spark df format
    ebdf = spark.createDataFrame(zip(srclist, dstlist, eblist), ebSchema)

    return ebdf



def clustering_coeff(g):
    """
    Takes as input :
        a Graphframe graph g
            
    Returns:
    
    """
    
    
    triangles = g.triangleCount()
    degree_links = g.degrees.withColumn('links', f.col('degree') * ( f.col('degree') - 1))
    cc = triangles.select("id", "count").join(degree_links, on='id')
    return cc.withColumn("clustering_coef", f.col('count') / (f.col('links') - (2 / f.col('degree'))) )


def overall_clustering_coefficient(g):
    """
    Takes as input :
        a Graphframe graph g
            
    Returns:
    
    """
    
    
        # dataframe containing num_triangles
        num_triangles_frame = g.triangleCount()
        
        # dataframe containing degrees
        degrees_frame = g.inDegrees
        
        
        # caculate the number of triangles, x3
        row_triangles = num_triangles_frame.agg({"count":"sum"}).collect()[0]
        num_triangles = row_triangles.asDict()['sum(count)']
        
        # calculate the number of triples
        degrees_frame = degrees_frame.withColumn("triples", 
                                                 degrees_frame.inDegree*(degrees_frame.inDegree-1)/2.0)
        row_triples = degrees_frame.agg({"triples":"sum"}).collect()[0]
        num_triples = row_triples.asDict()['sum(triples)']
        
        
        if num_triples==0:
            return 0
        else:
            return (1.0*num_triangles/num_triples)
