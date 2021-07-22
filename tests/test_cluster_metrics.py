from pyspark.sql import Row

from splink_graph.cluster_metrics import (
    cluster_main_stats,
    cluster_basic_stats,
    cluster_eb_modularity,
    cluster_lpg_modularity,
    cluster_avg_edge_betweenness,
    cluster_connectivity_stats,
)

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pyspark.sql.functions as f
import json


def test_cluster_basic_stats(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": "a", "dst": "b", "weight": 0.4, "cluster_id": 1},
        {"src": "b", "dst": "c", "weight": 0.56, "cluster_id": 1},
        {"src": "d", "dst": "e", "weight": 0.2, "cluster_id": 2},
        {"src": "f", "dst": "e", "weight": 0.8, "cluster_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_basic_stats(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
        weight_colname="weight",
    ).toPandas()

    assert (df_result["nodecount"] == 3).all()
    assert (df_result["edgecount"] == 2).all()
    assert df_result["density"].values == pytest.approx(0.666667, 0.01)


def test_cluster_main_stats(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "cluster_id": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "cluster_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = cluster_main_stats(e_df).toPandas()

    assert df_result["diameter"][0] == 2
    assert df_result["diameter"][1] == 2

    assert df_result["transitivity"][0] == pytest.approx(0, 0.01)
    assert df_result["transitivity"][1] == pytest.approx(0, 0.01)

    assert df_result["graphhash"][0] == "0f43d8cdd43b0b78727b192b6d6d0d0e"
    assert df_result["graphhash"][1] == "0f43d8cdd43b0b78727b192b6d6d0d0e"


def test_cluster_main_stats_customcolname(spark):
    # Create an Edge DataFrame with "id_l" and "id_r" columns
    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "cluster_id": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "cluster_id": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "cluster_id": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "cluster_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = cluster_main_stats(e_df, src="id_l", dst="id_r").toPandas()

    assert df_result["diameter"][0] == 2
    assert df_result["diameter"][1] == 2


def test_cluster_main_stats_customcolname2(spark):
    # Create an Edge DataFrame with "id_l" and "id_r" columns
    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "estimated_id": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "estimated_id": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "estimated_id": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "estimated_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = cluster_main_stats(
        e_df, src="id_l", dst="id_r", cluster_id_colname="estimated_id"
    ).toPandas()

    assert df_result["diameter"][0] == 2
    assert df_result["diameter"][1] == 2


def test_cluster_eb_modularity_neg(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 2 nodes one edge
    # when cut the nodes are singletons. modularity should be negative
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_eb_modularity(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["cluster_eb_modularity"][0] < 0


def test_cluster_eb_modularity_pos_large(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 7 nodes connected one after the other
    # modularity should be relatively large here (>0.30) .all edges are bridges
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.8, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.86, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.8, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.8, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.8, "cluster_id": 1},
        {"src": "f", "dst": "g", "distance": 0.8, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_eb_modularity(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["cluster_eb_modularity"][0] > 0.30


def test_cluster_eb_modularity_pos_small(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 6 nodes each connected to all others
    # modularity should be quite small here
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.96, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "d", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "c", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "d", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "c", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "c", "dst": "f", "distance": 0.94, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_eb_modularity(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert abs(df_result["cluster_eb_modularity"][0] - 0.01) < 0.1


def test_cluster_avg_cluster_eb(spark):
    # Create an Edge DataFrame with on "src" and "dst" column.
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.8, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.86, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.8, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.8, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.8, "cluster_id": 1},
        {"src": "f", "dst": "g", "distance": 0.8, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_avg_edge_betweenness(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["avg_cluster_eb"][0] > 0.4


def test_cluster_avg_cluster_eb_completegraph(spark):
    # Create an Edge DataFrame with on "src" and "dst" column.
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.96, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "d", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "c", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "d", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "c", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "c", "dst": "f", "distance": 0.94, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_avg_edge_betweenness(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["avg_cluster_eb"][0] == pytest.approx(0.076)


def test_cluster_avg_cluster_eb_0_156_othergraph(spark):
    # Create an Edge DataFrame with on "src" and "dst" column.
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.96, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "b", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "c", "dst": "f", "distance": 0.94, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_avg_edge_betweenness(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["avg_cluster_eb"][0] == pytest.approx(0.156)


def test_cluster_avg_cluster_eb_one_edge(spark):
    # Create an Edge DataFrame with on "src" and "dst" column.
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.8, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_avg_edge_betweenness(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["avg_cluster_eb"][0] == pytest.approx(1.00)


def test_cluster_lpg_modularity_zero(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 2 nodes one edge
    # with lpg both belong to same cluster. modularity should be 0
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_lpg_modularity(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["cluster_lpg_modularity"][0] == pytest.approx(0.0)


def test_cluster_lpg_modularity_pos_large(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 7 nodes connected one after the other
    # modularity should be relatively large here (>0.30) .all edges are bridges
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.8, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.86, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.8, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.8, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.8, "cluster_id": 1},
        {"src": "f", "dst": "g", "distance": 0.8, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_lpg_modularity(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["cluster_lpg_modularity"][0] > 0.30


def test_cluster_lpg_modularity_pos_small(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 6 nodes each connected to all others
    # modularity should be quite small here
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.96, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "d", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "c", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "d", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "c", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "c", "dst": "f", "distance": 0.94, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_lpg_modularity(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["cluster_lpg_modularity"][0] == pytest.approx(0.0, 0.001)


def test_cluster_connectivity_stats_completegraph(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 6 nodes each connected to all others
    # modularity should be quite small here
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.96, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "d", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "c", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "d", "distance": 0.92, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "d", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "a", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "a", "dst": "f", "distance": 0.94, "cluster_id": 1},
        {"src": "b", "dst": "f", "distance": 0.92, "cluster_id": 1},
        {"src": "c", "dst": "e", "distance": 0.98, "cluster_id": 1},
        {"src": "c", "dst": "f", "distance": 0.94, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_connectivity_stats(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["global_efficiency"][0] == pytest.approx(0.967, 0.001)
    assert df_result["node_conn"][0] == 4
    assert df_result["edge_conn"][0] == 4
    assert df_result["algebraic_conn"][0] == 4


def test_cluster_connectivity_stats_linegraph(spark):
    # Create an Edge DataFrame with on "src" and "dst" column. so 6 nodes each connected to all others

    data_list = [
        {"src": "a", "dst": "b", "distance": 0.8, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.86, "cluster_id": 1},
        {"src": "c", "dst": "d", "distance": 0.8, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.8, "cluster_id": 1},
        {"src": "e", "dst": "f", "distance": 0.8, "cluster_id": 1},
        {"src": "f", "dst": "g", "distance": 0.8, "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_connectivity_stats(
        e_df,
        src="src",
        dst="dst",
        distance_colname="distance",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["global_efficiency"][0] == pytest.approx(0.531, 0.001)
    assert df_result["node_conn"][0] == 1
    assert df_result["edge_conn"][0] == 1
    assert df_result["algebraic_conn"][0] < 0.2
