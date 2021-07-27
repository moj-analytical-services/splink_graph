import networkx as nx
import pyspark
import warnings
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
import networkx as nx
from graphframes import GraphFrame
from splink_graph.utils import nodes_from_edge_df


def _find_graphframes_jars(spark: SparkSession):
    try:
        from graphframes import GraphFrame
    except ImportError:
        raise ImportError(
            "need to import graphframes. ie from graphframes import Graphframes"
            "if you dont have it you need first to install graphframes from PyPI"
            "i.e.  pip install graphframes==0.6.0 "
        )

    sparkjars_conf = spark.conf.get("spark.jars")

    if "graphframes" not in sparkjars_conf:
        warnings.warn(
            "You need to include the path to the graphframes jar to your spark.jars conf settings"
            "when you define your 'sparkSession'"
            "Check related documentation on INSTALL.md on the splink_graph repo"
        )
        return 1

    if "logging" not in sparkjars_conf:
        warnings.warn(
            "You need to include the path to `scala-logging-api` and `scala-logging-slf4j` "
            "to your spark.jars conf settings when you define your 'sparkSession' "
            "Check related documentation on INSTALL.md on the splink_graph repo"
        )
        return 2

    return 0


def graphframes_connected_components(
    edges_df,
    src="src",
    dst="dst",
    weight_colname="tf_adjusted_match_prob",
    cluster_id_colname="cluster_id",
    cc_threshold=0.90,
):

    edges_for_cc = edges_df.filter(f.col(weight_colname) > cc_threshold)
    nodes_for_cc = nodes_from_edge_df(
        edges_for_cc, src="src", dst="dst", id_colname="id"
    )

    g = GraphFrame(nodes_for_cc, edges_for_cc)
    cc = g.connectedComponents()

    cc = cc.withColumnRenamed("component", cluster_id_colname).withColumnRenamed(
        "id", "node_id"
    )

    return cc


def pyspark_connected_components(
    spark,
    edges_df,
    src,
    dst,
    weight_colname="tf_adjusted_match_prob",
    cluster_id_colname="cluster_id",
    cc_threshold=0.90,
    checkpoint_dir="graphframes_tempdir/",
    checkpoint_every=2,
    max_n=None,
):

    edges_for_cc = edges_df.filter(f.col(weight_colname) > cc_threshold)

    # get all nodes from edge dataframe
    nodes_for_cc = nodes_from_edge_df(edges_for_cc, src=src, dst=dst, id_colname="id")

    # add a monotonically_increasing_id so we can create a numberedgelist
    nodes_for_cc = nodes_for_cc.withColumn("numid", f.monotonically_increasing_id())

    # get the edgelist to be from [src,dst] to [numidsrc,numiddst]
    temp1 = edges_for_cc.join(nodes_for_cc, edges_for_cc["src"] == nodes_for_cc["id"])
    temp1 = temp1.withColumnRenamed("numid", "numidsrc").drop("src", "id")
    temp2 = temp1.join(nodes_for_cc, temp1["dst"] == nodes_for_cc["id"])
    edgenumlist = temp2.withColumnRenamed("numid", "numiddst").drop("dst", "id")

    # set up a few strings for use further down the line
    label = "node_id"
    a = "numidsrc"
    b = "numiddst"

    b2 = b + "join"
    filepattern = "pyspark_cc_iter{}.parquet"

    # cache the edge numlist because it will be used many times
    adj_cache = edgenumlist.persist()

    # for each id in the `a` column, take the minimum of the
    # ids in the `b` column

    mins = (
        adj_cache.groupby(a)
        .agg(f.min(f.col(b)).alias("min1"))
        .withColumn("change", f.lit(0))
        .persist()
    )

    if max_n is not None:
        # ensure a global minimum id that is less than
        # any ids in the dataset
        minimum_id = (
            mins.select(f.min(f.col("min1")).alias("n")).collect()[0]["n"] - 1000000
        )  # transformation used repeatedly to replace old labels with new
    fill_in = f.coalesce(f.col("min2"), f.col("min1"))
    # join conditions for comparing nodes
    criteria = [
        f.col(b) == f.col(b2),
        f.col("min1") > f.col("min3"),
    ]

    # initial conditions for the loop
    not_done = mins.count()
    old_done = not_done + 1
    new_done = not_done
    niter = 0
    used_filepaths = []
    filepath = None

    # repeat until done
    while not_done > 0:
        niter += 1

        if max_n is not None:
            min_a = mins.filter(f.col("min1") != minimum_id)
        else:
            min_a = mins  # get new minimum ids
            newmins = (
                adj_cache.join(min_a, a, "inner")
                .join(
                    mins.select(f.col(a).alias(b2), f.col("min1").alias("min3")),
                    criteria,
                    "inner",
                )
                .groupby(f.col("min1").alias("min1"))
                .agg(f.min(f.col("min3")).alias("min2"))
            )
        # reconcile new minimum ids with the
        # minimum ids from the previous iteration

        mins = mins.join(newmins, "min1", "left").select(
            a, fill_in.alias("min1"), (f.col("min1") != fill_in).alias("change")
        )
        # if there is a max_n, assign the global minimum id to
        # any components with more than max_n nodes

        if max_n is not None:
            mins = (
                mins.withColumn(
                    "n",
                    f.count(f.col(a)).over(
                        w.partitionBy(f.col("min1")).orderBy(f.lit(None))
                    ),
                )
                .withColumn(
                    "min1",
                    f.when(f.col("n") >= max_n, f.lit(minimum_id)).otherwise(
                        f.col("min1")
                    ),
                )
                .drop("n")
            )

            # logic for deciding whether to persist of checkpoint
            if (niter % checkpoint_every) == 0:
                filepath = checkpoint_dir + filepattern.format(niter)
                used_filepaths.append(filepath)
                mins.write.parquet(filepath, mode="overwrite")
                mins = spark.read.parquet(filepath)
            else:
                mins = mins.persist()  # update inputs for stopping logic

        not_done = mins.filter(f.col("change") == True).count()
        old_done = new_done
        new_done = not_done
        n_components = mins.select(f.countDistinct(f.col("min1")).alias("n")).collect()[
            0
        ]["n"]

        print(niter, not_done, n_components)

        output = mins.select(f.col(a).alias(label), f.col("min1").alias("component"))

        if max_n is not None:

            output = output.withColumn(
                "component",
                f.when(f.col("component") == minimum_id, f.lit(None)).otherwise(
                    f.col("component")
                ),
            )

        output = output.persist()

        n_components = output.select(
            f.countDistinct(f.col("component")).alias("n")
        ).collect()[0]["n"]

    print("total components:", n_components)

    cc = (
        output.join(nodes_for_cc, output["node_id"] == nodes_for_cc["numid"])
        .drop("numid")
        .withColumnRenamed("component", cluster_id_colname)
    )

    return cc
