from pyspark.sql import Row
import pyspark
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from splink_graph.cc import (
    _find_graphframes_jars,
    graphframes_connected_components,
    nx_connected_components,
)
from graphframes import GraphFrame
import networkx as nx


@pytest.mark.order(1)
def test_cc_simple(sparkSessionwithgraphframes, graphframes_tmpdir):

    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = sparkSessionwithgraphframes.createDataFrame(
        [
            ("a", "b", 0.4),
            ("b", "c", 0.56),
            ("d", "e", 0.84),
            ("e", "f", 0.65),
            ("f", "d", 0.67),
            ("f", "g", 0.34),
            ("g", "h", 0.99),
            ("h", "i", 0.5),
            ("h", "j", 0.8),
        ],
        ["src", "dst", "weight"],
    )

    assert _find_graphframes_jars(sparkSessionwithgraphframes) == 0

    sparkSessionwithgraphframes.sparkContext.setCheckpointDir("graphframes_tempdir/")
    df_result = graphframes_connected_components(
        e2_df, src="src", dst="dst", weight_colname="weight", cc_threshold=0.82
    ).toPandas()

    assert df_result["cluster_id"].unique().size == 2
    assert df_result["node_id"].unique().size == 4

    df_result2 = graphframes_connected_components(
        e2_df, src="src", dst="dst", weight_colname="weight", cc_threshold=0.2
    ).toPandas()
    assert df_result2["cluster_id"].unique().size == 2
    assert df_result2["node_id"].count() == 10


def test_nx_cc_simple(spark):

    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = spark.createDataFrame(
        [
            ("a", "b", 0.4),
            ("b", "c", 0.56),
            ("d", "e", 0.84),
            ("e", "f", 0.65),
            ("f", "d", 0.67),
            ("f", "g", 0.34),
            ("g", "h", 0.99),
            ("h", "i", 0.5),
            ("h", "j", 0.8),
        ],
        ["src", "dst", "weight"],
    )

    df_result = nx_connected_components(
        spark, e2_df, src="src", dst="dst", weight_colname="weight", cc_threshold=0.82
    ).toPandas()

    assert df_result["cluster_id"].unique().size == 2
    assert df_result["node_id"].unique().size == 4

    df_result2 = nx_connected_components(
        spark, e2_df, src="src", dst="dst", weight_colname="weight", cc_threshold=0.2
    ).toPandas()
    assert df_result2["cluster_id"].unique().size == 2
    assert df_result2["node_id"].count() == 10


def test_nx_cc_2edges(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = spark.createDataFrame(
        [("a", "b", 0.999), ("y", "z", 0.999),], ["src", "dst", "weight"],
    )
    df_result = nx_connected_components(
        spark, e2_df, src="src", dst="dst", weight_colname="weight", cc_threshold=0.82
    ).toPandas()

    assert df_result["cluster_id"].unique().size == 2
