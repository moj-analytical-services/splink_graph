from pyspark.sql.types import (
    LongType,
    StringType,
    FloatType,
    IntegerType,
    StructType,
    StructField,
)
import pyspark.sql.functions as f
from pyspark.sql.functions import pandas_udf, PandasUDFType
import networkx as nx
import networkx.algorithms.community as nx_comm
from networkx.algorithms.distance_measures import diameter
from networkx.algorithms.cluster import transitivity
from networkx.algorithms.centrality import edge_betweenness_centrality
from networkx.algorithms.bridges import bridges
from networkx.algorithms.community.centrality import girvan_newman
from networkx.algorithms.components import articulation_points
import pandas as pd
import math
from splink_graph.utils import _laplacian_spectrum

# Read on how to setup spark to work around with pandas udf:
# see answers on
# https://stackoverflow.com/questions/58458415/pandas-scalar-udf-failing-illegalargumentexception


def cluster_basic_stats(
    df, src="src", dst="dst", cluster_id_colname="cluster_id", weight_colname="weight"
):

    """show nodecount , edgecount, density and enumerate nodes per cluster/conn. components subgraph

    example input spark dataframe:


    |src|dst|cluster_id|
    |---|---|----------|
    |  f|  d|         0|
    |  f|  g|         0|
    |  b|  c|8589934592|
    |  g|  h|         0|
    |  a|  b|8589934592|
    |  h|  i|         0|
    |  h|  j|         0|
    |  d|  e|         0|
    |  e|  f|         0|


    example output spark dataframe:

    |cluster_id|               nodes|nodecount|edgecount|density|
    |----------|--------------------|---------|---------|------|
    |8589934592|           [b, a, c]|        3|        2|0.666|
    |         0|[h, g, f, e, d, i..]|        7|        7|0.333|


    """

    edgec = df.groupby(cluster_id_colname).agg(
        f.count(weight_colname).alias("edgecount")
    )
    srcdf = df.groupby(cluster_id_colname).agg(f.collect_set(src).alias("sources"))
    dstdf = df.groupby(cluster_id_colname).agg(f.collect_set(dst).alias("destinations"))
    allnodes = srcdf.join(dstdf, on=cluster_id_colname)
    allnodes = allnodes.withColumn(
        "nodes", f.array_union(f.col("sources"), f.col("destinations"))
    ).withColumn("nodecount", f.size(f.col("nodes")))

    output = allnodes.join(edgec, on=cluster_id_colname)

    # density related calcs based on nodecount and max possible number of edges in an undirected graph

    output = output.withColumn(
        "maxNumberOfEdgesundir", f.expr("(nodecount*(nodecount-1))/2")
    )
    output = output.withColumn(
        "density", f.expr("edgecount/maxNumberOfEdgesundir")
    ).drop("sources", "destinations", "maxNumberOfEdgesundir")
    output = output.withColumnRenamed(cluster_id_colname, "cluster_id")

    return output


def cluster_graph_hash(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id"):
    """calculate weisfeiler-lehman graph hash of a cluster


    Args:
        sparkdf: imput edgelist Spark DataFrame
        src: src column name
        dst: dst column name
        cluster_id_colname: Graphframes-created connected components created cluster_id
    """
    psrc = src
    pdst = dst

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("graphhash", StringType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def gh(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)
        h = nx.weisfeiler_lehman_graph_hash(nxGraph)
        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [h]],
            columns=[
                "cluster_id",
                "graphhash",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(gh)
    return out


def cluster_graph_hash_edge_attr(
    sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id", edge_attr_col=None
):
    """calculate weisfeiler-lehman graph hash of a cluster taking into account the edge weights too.
      weights are converted to strings for the hashing.


    Args:
        sparkdf: imput edgelist Spark DataFrame
        src: src column name
        dst: dst column name
        cluster_id_colname: Graphframes-created connected components created cluster_id
        edge_attr_col: edge attributes (like edge weights) column
    """
    psrc = src
    pdst = dst

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("graphhash_ea", StringType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def gh_edge_attr(pdf: pd.DataFrame) -> pd.DataFrame:

        if edge_attr_col:
            pdf[edge_attr_col] = pdf[edge_attr_col].astype(str)

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, [edge_attr_col])

        ghe = nx.weisfeiler_lehman_graph_hash(nxGraph, edge_attr=edge_attr_col)
        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [ghe]],
            columns=[
                "cluster_id",
                "graphhash_ea",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(gh_edge_attr)
    return out


def cluster_main_stats(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id"):
    """calculate diameter / transitivity(GCC) / triangle clustering coefficient LCC / square clustering coeff

        Args:
            sparkdf: imput edgelist Spark DataFrame
            src: src column name
            dst: dst column name
            cluster_id_colname: Graphframes-created connected components created cluster_id



        input spark dataframe:


    |src|dst|weight|cluster_id|distance|
    |---|---|------|----------|--------|
    |  f|  d|  0.67|         0| 0.329|
    |  f|  g|  0.34|         0| 0.659|
    |  b|  c|  0.56|8589934592| 0.439|
    |  g|  h|  0.99|         0|0.010|
    |  a|  b|   0.4|8589934592|0.6|
    |  h|  i|   0.5|         0|0.5|
    |  h|  j|   0.8|         0| 0.199|
    |  d|  e|  0.84|         0| 0.160|
    |  e|  f|  0.65|         0|0.35|



        output spark dataframe:




    """

    psrc = src
    pdst = dst

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("diameter", IntegerType()),
                StructField("transitivity", FloatType()),
                StructField("tri_clustcoeff", FloatType()),
                StructField("sq_clustcoeff", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def drt(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)
        d = diameter(nxGraph)
        t = transitivity(nxGraph)
        tric = nx.average_clustering(nxGraph)
        sq = nx.square_clustering(nxGraph)
        sqc = sum(sq.values()) / len(sq.values())

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [d] + [t] + [tric] + [sqc]],
            columns=[
                "cluster_id",
                "diameter",
                "transitivity",
                "tri_clustcoeff",
                "sq_clustcoeff",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(drt)

    out = (
        out.withColumn("tri_clustcoeff", f.round(f.col("tri_clustcoeff"), 3))
        .withColumn("sq_clustcoeff", f.round(f.col("sq_clustcoeff"), 3))
        .withColumn("transitivity", f.round(f.col("transitivity"), 3))
    )
    return out


def cluster_connectivity_stats(
    sparkdf,
    src="src",
    dst="dst",
    cluster_id_colname="cluster_id",
):
    """outputs connectivity metrics per cluster_id

        Args:
            sparkdf: imput edgelist Spark DataFrame
            src: src column name
            dst: dst column name
            cluster_id_colname: Graphframes-created connected components created cluster_id

        Returns:

            node_conn: Node Connectivity measures the minimal number of vertices that can be removed to disconnect the graph.
            edge_conn: Edge connectivity measures the minimal number of edges that can be removed to disconnect the graph.
            degeneracy: a way to measure sparsity
            num_articulation_pts: how many articulation points? an articulation point is a node that if removed disconnects a graph


        The larger these metrics are --> the more connected the subggraph is.



        input spark dataframe:


    |src|dst|weight|cluster_id|distance|
    |---|---|------|----------|--------|
    |  f|  d|  0.67|         0| 0.329|
    |  f|  g|  0.34|         0| 0.659|
    |  b|  c|  0.56|8589934592| 0.439|
    |  g|  h|  0.99|         0|0.010|
    |  a|  b|   0.4|8589934592|0.6|
    |  h|  i|   0.5|         0|0.5|
    |  h|  j|   0.8|         0| 0.199|
    |  d|  e|  0.84|         0| 0.160|
    |  e|  f|  0.65|         0|0.35|



        output spark dataframe:

    """

    psrc = src
    pdst = dst

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("node_conn", IntegerType()),
                StructField("edge_conn", IntegerType()),
                StructField("degeneracy", IntegerType()),
                StructField("num_articulation_pts", IntegerType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def conn_eff(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)
        nc = nx.algorithms.node_connectivity(nxGraph)
        ec = nx.algorithms.edge_connectivity(nxGraph)
        dg = max(nx.core_number(nxGraph).values())

        ap = articulation_points(nxGraph)
        num_aps = len(list(ap))

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [nc] + [ec] + [dg] + [num_aps]],
            columns=[
                "cluster_id",
                "node_conn",
                "edge_conn",
                "degeneracy",
                "num_articulation_pts",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(conn_eff)

    return out


def cluster_eb_modularity(
    sparkdf,
    src="src",
    dst="dst",
    distance_colname="distance",
    cluster_id_colname="cluster_id",
):
    """
        Args:
            sparkdf: imput edgelist Spark DataFrame
            src: src column name
            dst: dst column name
            distance_colname: column name where edge distance (1-weight) is available
            cluster_id_colname: column that contains Graphframes-created connected components created cluster_id

        Returns:
            cluster_id: connected components created cluster_id
            comp_eb_modularity: modularity for cluster_id if it partitioned into 2 parts at the point where the highest edge betweenness exists


        example input spark dataframe


    |src|dst|weight|cluster_id|distance|
    |---|---|------|----------|--------|
    |  f|  d|  0.67|         0| 0.329|
    |  f|  g|  0.34|         0| 0.659|
    |  b|  c|  0.56|8589934592| 0.439|
    |  g|  h|  0.99|         0|0.010|
    |  a|  b|   0.4|8589934592|0.6|
    |  h|  i|   0.5|         0|0.5|
    |  h|  j|   0.8|         0| 0.199|
    |  d|  e|  0.84|         0| 0.160|
    |  e|  f|  0.65|         0|0.35|


        example output spark dataframe

    |cluster_id|comp_eb_modularity|
    |----------|----------------|
    |         0| 0.400|
    |8589934592| -0.04|

    """

    psrc = src
    pdst = dst
    pdistance = distance_colname

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("cluster_eb_modularity", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def cluster_eb_m(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)

        ## TODO: document this code
        # this is a method that calculates the modularity of a cluster if partitioned into 2 parts
        # where the split is happening where the highest edge betweeness is.

        # if modularity is negative :
        #      that means that the split just leaves singleton nodes or something like that.
        #      basically the cluster is of no interest
        # if modularity is 0 or very close to 0 :
        #      its a cluster of well connected nodes so... nothing to see here really.
        # if modularity is around 0.3+ then :
        #      its a cluster of possible interest

        def largest_edge_betweenness(G):
            centrality = edge_betweenness_centrality(
                G, weight=pdistance, normalized=True
            )
            return max(centrality, key=centrality.get)

        comp = girvan_newman(nxGraph, most_valuable_edge=largest_edge_betweenness)
        gn = tuple(sorted(c) for c in next(comp))

        co = pdf[cluster_id_colname].iloc[0]  # access component id
        nc = nx.number_of_nodes(nxGraph)

        if nc > 2:
            try:
                co_eb_mod = nx_comm.modularity(nxGraph, gn)
            except ZeroDivisionError:
                raise Exception(
                    f"ZeroDivisionError on component id {co}. "
                    "This can occur if one of the weights (distances) is zero."
                )
        else:
            co_eb_mod = -1.0

        return pd.DataFrame(
            [[co] + [co_eb_mod]],
            columns=[
                "cluster_id",
                "cluster_eb_modularity",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(cluster_eb_m)

    return out


def cluster_lpg_modularity(
    sparkdf,
    src="src",
    dst="dst",
    distance_colname="distance",
    cluster_id_colname="cluster_id",
):
    """
        Args:
            sparkdf: imput edgelist Spark DataFrame
            src: src column name
            dst: dst column name
            distance_colname: column name where edge distance (1-weight) is available
            cluster_id_colname: column that contains Graphframes-created connected components created cluster_id

        Returns:
            cluster_id: connected components created cluster_id
            cluster_lpg_modularity: modularity for cluster_id if it partitioned into 2 parts based on label propagation


        example input spark dataframe


    |src|dst|weight|cluster_id|distance|
    |---|---|------|----------|--------|
    |  f|  d|  0.67|         0| 0.329|
    |  f|  g|  0.34|         0| 0.659|
    |  b|  c|  0.56|8589934592| 0.439|
    |  g|  h|  0.99|         0|0.010|
    |  a|  b|   0.4|8589934592|0.6|
    |  h|  i|   0.5|         0|0.5|
    |  h|  j|   0.8|         0| 0.199|
    |  d|  e|  0.84|         0| 0.160|
    |  e|  f|  0.65|         0|0.35|


        example output spark dataframe

    |cluster_id|cluster_lpg_modularity|
    |----------|----------------|
    |         0| 0.400|
    |8589934592| -0.04|

    """

    psrc = src
    pdst = dst
    pdistance = distance_colname

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("cluster_lpg_modularity", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def cluster_lpg_m(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)

        ## TODO: document this code
        # this is a method that calculates the modularity of a cluster if partitioned into 2 parts
        # where the split is happening based on label propagation clustering.

        # if modularity is negative :
        #      that means that the split just leaves singleton nodes or something like that.
        #      basically the cluster is of no interest
        # if modularity is 0 or very close to 0 :
        #      its a cluster of well connected nodes so... nothing to see here really.
        # if modularity is around 0.3+ then :
        #      its a cluster of possible interest

        gn = list(nx_comm.label_propagation_communities(nxGraph))

        co = pdf[cluster_id_colname].iloc[0]  # access component id
        co_lpg_mod = nx_comm.modularity(nxGraph, gn)

        return pd.DataFrame(
            [[co] + [co_lpg_mod]],
            columns=[
                "cluster_id",
                "cluster_lpg_modularity",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(cluster_lpg_m)

    return out


def cluster_avg_edge_betweenness(
    sparkdf,
    src="src",
    dst="dst",
    distance_colname="distance",
    cluster_id_colname="cluster_id",
):
    """
        Args:
            sparkdf: imput edgelist Spark DataFrame
            src: src column name
            dst: dst column name
            distance_colname: column name where edge distance (1-weight) is available
            cluster_id_colname: column that contains Graphframes-created connected components created cluster_id

        Returns:
            cluster_id: connected components created cluster_id
            avg_cluster_eb: average edge betweeness for cluster_id

        input spark dataframe:


    |src|dst|weight|cluster_id|distance|
    |---|---|------|----------|--------|
    |  f|  d|  0.67|         0| 0.329|
    |  f|  g|  0.34|         0| 0.659|
    |  b|  c|  0.56|8589934592| 0.439|
    |  g|  h|  0.99|         0|0.010|
    |  a|  b|   0.4|8589934592|0.6|
    |  h|  i|   0.5|         0|0.5|
    |  h|  j|   0.8|         0| 0.199|
    |  d|  e|  0.84|         0| 0.160|
    |  e|  f|  0.65|         0|0.35|



        output spark dataframe:

    """

    psrc = src
    pdst = dst
    pdistance = distance_colname

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("avg_cluster_eb", FloatType()),
                StructField("wiener_ind", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def avg_eb(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst, pdistance)
        w = nx.wiener_index(nxGraph)

        edge_btwn = edge_betweenness_centrality(nxGraph, normalized=True)

        if len(edge_btwn) > 0:
            aeb = round(sum(list(edge_btwn.values())) / len(edge_btwn), 3)
        else:
            aeb = 0.0

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [aeb] + [w]], columns=["cluster_id", "avg_cluster_eb", "wiener_ind"]
        )

    out = sparkdf.groupby(cluster_id_colname).apply(avg_eb)

    return out


def number_of_bridges(
    sparkdf,
    src="src",
    dst="dst",
    cluster_id_colname="cluster_id",
):

    """return

        Args:
            sparkdf: imput edgelist Spark DataFrame
            src: src column name
            dst: dst column name
            cluster_id_colname: Graphframes-created connected components created cluster_id

     Returns:
            cluster_id: Connected components created cluster_id
            num_bridges: The number of bridges in the cluster



    example input spark dataframe

    |src|dst|cluster_id|
    +---|---|----------|
    |  f|  d|         0|
    |  f|  g|         0|
    |  b|  c|8589934592|
    |  g|  h|         0|
    |  a|  b|8589934592|
    |  h|  i|         0|
    |  h|  j|         0|
    |  d|  e|         0|
    |  e|  f|         0|


    example output spark dataframe


    |   cluster_id |   number_of_bridges |
    |-------------:|--------------------:|
    |   8589934592 |                   2 |
    |            0 |                   4 |


    """
    psrc = src
    pdst = dst
    pcomponent = cluster_id_colname

    bridgesoutSchema = StructType(
        [
            StructField("cluster_id", LongType()),
            StructField("number_of_bridges", LongType()),
        ]
    )

    @pandas_udf(bridgesoutSchema, PandasUDFType.GROUPED_MAP)
    def br_p_udf(pdf: pd.DataFrame) -> pd.DataFrame:

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)

        b = bridges(nxGraph)

        data = {"cluster_id": [co], "number_of_bridges": [len(list(b))]}

        return pd.DataFrame(data)

    indf = sparkdf.select(psrc, pdst, pcomponent)
    out = indf.groupby(cluster_id_colname).apply(br_p_udf)
    return out


def cluster_assortativity(
    sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id"
):
    """calculate assortativity of a cluster

        Args:
            sparkdf: imput edgelist Spark DataFrame
            src: src column name
            dst: dst column name
            cluster_id_colname: Graphframes-created connected components created cluster_id



        input spark dataframe:


    |src|dst|weight|cluster_id|distance|
    |---|---|------|----------|--------|
    |  f|  d|  0.67|         0| 0.329|
    |  f|  g|  0.34|         0| 0.659|
    |  b|  c|  0.56|8589934592| 0.439|
    |  g|  h|  0.99|         0|0.010|
    |  a|  b|   0.4|8589934592|0.6|
    |  h|  i|   0.5|         0|0.5|
    |  h|  j|   0.8|         0| 0.199|
    |  d|  e|  0.84|         0| 0.160|
    |  e|  f|  0.65|         0|0.35|



        output spark dataframe:

    """

    psrc = src
    pdst = dst

    @pandas_udf(
        StructType(
            [
                StructField("cluster_id", LongType()),
                StructField("assortativity", FloatType()),
            ]
        ),
        functionType=PandasUDFType.GROUPED_MAP,
    )
    def asrt(pdf: pd.DataFrame) -> pd.DataFrame:

        nxGraph = nx.Graph()
        nxGraph = nx.from_pandas_edgelist(pdf, psrc, pdst)

        r = nx.degree_pearson_correlation_coefficient(nxGraph)

        # when all degrees are equal (grids or full graphs) assortativity is undefined/nan
        # However we can think this case as fully correlated so we put it as 1

        if math.isnan(r):
            r = 1.0

        co = pdf[cluster_id_colname].iloc[0]  # access component id

        return pd.DataFrame(
            [[co] + [r]],
            columns=[
                "cluster_id",
                "assortativity",
            ],
        )

    out = sparkdf.groupby(cluster_id_colname).apply(asrt)

    out = out.withColumn("assortativity", f.round(f.col("assortativity"), 3))

    return out
