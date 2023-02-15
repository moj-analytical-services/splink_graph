from pyspark.sql import Row
import networkx as nx
from splink_graph.cluster_metrics import (
    cluster_main_stats,
    cluster_basic_stats,
    cluster_eb_modularity,
    cluster_lpg_modularity,
    cluster_avg_edge_betweenness,
    cluster_connectivity_stats,
    number_of_bridges,
    cluster_graph_hash,
    cluster_graph_hash_edge_attr,
    cluster_assortativity,
)

import pytest
import math
import pandas as pd
import pyspark.sql.functions as f


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


def test_cluster_eb_modularity_0_dist(spark):
    with pytest.raises(Exception):
        # Create an Edge DataFrame with on "src" and "dst" column. so 7 nodes connected one after the other
        # modularity should be relatively large here (>0.30) .all edges are bridges
        data_list = [
            {"src": "a", "dst": "b", "distance": 1.0, "cluster_id": 1},
            {"src": "b", "dst": "c", "distance": 0.00, "cluster_id": 1},
            {"src": "c", "dst": "d", "distance": 1.0, "cluster_id": 1},
            {"src": "d", "dst": "e", "distance": 1.0, "cluster_id": 1},
            {"src": "e", "dst": "f", "distance": 1.0, "cluster_id": 1},
            {"src": "f", "dst": "g", "distance": 1.0, "cluster_id": 1},
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
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["node_conn"][0] == 4
    assert df_result["edge_conn"][0] == 4
    assert df_result["degeneracy"][0] == 4


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
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["node_conn"][0] == 1
    assert df_result["edge_conn"][0] == 1
    assert df_result["degeneracy"][0] == 1


def test_connectivity_degen_barbell(spark):

    g = nx.barbell_graph(5, 3)
    fourbridges = pd.DataFrame(list(g.edges), columns=["src", "dst"])
    fourbridges["weight"] = 1.0
    fourbridges["cluster_id"] = 1

    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = spark.createDataFrame(
        fourbridges,
        ["src", "dst", "weight", "cluster_id"],
    )

    e2_df = e2_df.withColumn("distance", 1.0 - f.col("weight"))

    df_result = cluster_connectivity_stats(
        e2_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["node_conn"][0] == 1
    assert df_result["edge_conn"][0] == 1
    assert df_result["degeneracy"][0] == 4


def test_number_of_bridges(spark):

    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = spark.createDataFrame(
        [
            ("a", "b", 0.4, 1),
            ("b", "c", 0.56, 1),
            ("d", "e", 0.84, 2),
            ("e", "f", 0.65, 2),
            ("f", "d", 0.67, 2),
            ("f", "g", 0.34, 2),
            ("g", "h", 0.99, 2),
            ("h", "i", 0.5, 2),
            ("h", "j", 0.8, 2),
        ],
        ["src", "dst", "weight", "clus_id"],
    )

    e2_df = e2_df.withColumn("distance", 1.0 - f.col("weight"))

    out = number_of_bridges(e2_df, cluster_id_colname="clus_id").toPandas()

    filter_cluster_1 = out["cluster_id"] == 1

    filter_cluster_2 = out["cluster_id"] == 2

    num_bridges_cluster_1 = out[filter_cluster_1].iloc[0]["number_of_bridges"]
    assert num_bridges_cluster_1 == 2

    num_bridges_cluster_2 = out[filter_cluster_2].iloc[0]["number_of_bridges"]
    assert num_bridges_cluster_2 == 4


def test_four_bridges(spark):

    g = nx.barbell_graph(5, 3)
    fourbridges = pd.DataFrame(list(g.edges), columns=["src", "dst"])
    fourbridges["weight"] = 1.0
    fourbridges["cluster_id"] = 1

    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = spark.createDataFrame(
        fourbridges,
        ["src", "dst", "weight", "cluster_id"],
    )

    e2_df = e2_df.withColumn("distance", 1.0 - f.col("weight"))

    result = number_of_bridges(e2_df, cluster_id_colname="cluster_id").toPandas()

    assert result["number_of_bridges"][0] == 4


def test_0_bridges(spark):

    g = nx.complete_graph(9)
    zerobridges = pd.DataFrame(list(g.edges), columns=["src", "dst"])
    zerobridges["weight"] = 1.0
    zerobridges["cluster_id"] = 1

    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = spark.createDataFrame(
        zerobridges,
        ["src", "dst", "weight", "cluster_id"],
    )

    e2_df = e2_df.withColumn("distance", 1.0 - f.col("weight"))

    result = number_of_bridges(e2_df, cluster_id_colname="cluster_id").toPandas()

    assert result["number_of_bridges"][0] == 0


def test_cluster_graph_hash(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": "a", "dst": "b", "weight": 0.4, "cluster_id": 1},
        {"src": "b", "dst": "c", "weight": 0.56, "cluster_id": 1},
        {"src": "d", "dst": "e", "weight": 0.2, "cluster_id": 2},
        {"src": "f", "dst": "e", "weight": 0.8, "cluster_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_graph_hash(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["graphhash"][0] == "5144181ac27497fdfa9bdb5b8b799630"


def test_cluster_graph_hash_attr(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": 0, "dst": 2, "weight": "3", "cluster_id": 1},
        {"src": 1, "dst": 2, "weight": "3", "cluster_id": 1},
        {"src": 2, "dst": 3, "weight": "3", "cluster_id": 1},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)

    df_result = cluster_graph_hash_edge_attr(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
        edge_attr_col="weight",
    ).toPandas()

    # nx.__version__ needs to be under 2.7 for this test to run sucessfully
    # in networkx 2.7 onwards the way of calculating the hash differs
    # networkx 2.7 however doesnt work with python 3.6, 3.7 but only 3.8 onwrds
    # hence for the time being a nx version less thank 2.7 is used in splink_graph

    assert df_result["graphhash_ea"][0] == "9483edf7fe6902d0cbdcbdbe6b7e6666"


def test_cluster_assortativity_neg(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": "a", "dst": "b", "weight": 0.4, "cluster_id": 1},
        {"src": "b", "dst": "c", "weight": 0.56, "cluster_id": 1},
        {"src": "d", "dst": "e", "weight": 0.2, "cluster_id": 2},
        {"src": "f", "dst": "e", "weight": 0.8, "cluster_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    df_result = cluster_assortativity(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["assortativity"][0] < 0.0


def test_cluster_assortativity_pos(spark):
    # Create an Edge DataFrame with "src" and "dst" columns from a graph generator
    g = nx.barbell_graph(5, 3)
    barb = pd.DataFrame(list(g.edges), columns=["src", "dst"])

    barb["cluster_id"] = 1

    # Create an spark Edge DataFrame with "src" and "dst" columns
    e_df = spark.createDataFrame(
        barb,
        ["src", "dst", "cluster_id"],
    )

    df_result = cluster_assortativity(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
    ).toPandas()

    assert df_result["assortativity"][0] > 0.0


def test_cluster_assortativity_fully(spark):
    # Create an Edge DataFrame with "src" and "dst" columns from a graph generator

    g = nx.complete_graph(9)
    zerobridges = pd.DataFrame(list(g.edges), columns=["src", "dst"])
    zerobridges["cluster_id"] = 1

    # Create an spark Edge DataFrame with "src" and "dst" columns

    e_df = spark.createDataFrame(
        zerobridges,
        ["src", "dst", "cluster_id"],
    )

    df_result = cluster_assortativity(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
    ).toPandas()

    # when all degrees are equal (grids or full graphs) assortativity is nan
    assert df_result["assortativity"][0] == 1


def test_cluster_num_aps_0(spark):
    # Create an Edge DataFrame with "src" and "dst" columns from a graph generator

    g = nx.complete_graph(9)
    zerobridges = pd.DataFrame(list(g.edges), columns=["src", "dst"])
    zerobridges["cluster_id"] = 1

    # Create an spark Edge DataFrame with "src" and "dst" columns

    e_df = spark.createDataFrame(
        zerobridges,
        ["src", "dst", "cluster_id"],
    )

    df_result = cluster_connectivity_stats(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
    ).toPandas()

    # when we have a complete graph num_articulation_pts is 0
    assert df_result["num_articulation_pts"][0] == 0


def test_cluster_num_aps_3(spark):
    # Create an Edge DataFrame with "src" and "dst" columns from a graph generator

    g = nx.barbell_graph(5, 1)
    nxgraph = pd.DataFrame(list(g.edges), columns=["src", "dst"])
    nxgraph["cluster_id"] = 1

    # Create an spark Edge DataFrame with "src" and "dst" columns

    e_df = spark.createDataFrame(
        nxgraph,
        ["src", "dst", "cluster_id"],
    )

    df_result = cluster_connectivity_stats(
        e_df,
        src="src",
        dst="dst",
        cluster_id_colname="cluster_id",
    ).toPandas()

    # when we have a complete graph num_articulation_pts is 0
    assert df_result["num_articulation_pts"][0] == 3
