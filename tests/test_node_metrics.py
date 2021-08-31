from pyspark.sql import Row
from splink_graph.node_metrics import eigencentrality, harmoniccentrality
import pytest
import pandas as pd
import networkx as nx
from pandas.testing import assert_frame_equal
import pyspark.sql.functions as f
import json


def test_eigencentrality_simple(spark):
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "cluster_id": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "cluster_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = eigencentrality(e_df).toPandas()

    assert df_result["eigen_centrality"][0] == pytest.approx(0.50000, 0.01)


def test_eigencentrality_star(spark):

    g = nx.star_graph(8)
    star = pd.DataFrame(list(g.edges), columns=["src", "dst"])
    star = star.astype(
        {"src": str, "dst": str}
    )  # because the src and dst come from star_graph and look like ints
    star["weight"] = 1.0
    star["cluster_id"] = 1

    # Create an Edge DataFrame with "src" and "dst" columns
    e_df = spark.createDataFrame(star, ["src", "dst", "weight", "cluster_id"],)

    df_result = eigencentrality(e_df).toPandas()

    # assert df_result["eigen_centrality"][0] == pytest.approx(0.50000, 0.01)


def test_eigencentrality_customcolname(spark):

    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "clus_id": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "clus_id": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "clus_id": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "clus_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = eigencentrality(
        e_df, src="id_l", dst="id_r", cluster_id_colname="clus_id"
    ).toPandas()

    assert df_result["eigen_centrality"][0] == pytest.approx(0.50000, 0.01)

    f1 = df_result["node_id"] == "a"
    assert df_result.loc[f1, "clus_id"][0] == 1


def test_harmoniccentrality_simple(spark):
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "cluster_id": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "cluster_id": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "cluster_id": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "cluster_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = harmoniccentrality(e_df).toPandas()

    assert df_result["harmonic_centrality"][0] == pytest.approx(1.50, 0.1)
    assert df_result["harmonic_centrality"][4] == pytest.approx(2.0, 0.1)


def test_harmoniccentrality_customcolname(spark):
    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "clus_id": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "clus_id": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "clus_id": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "clus_id": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = harmoniccentrality(
        e_df, src="id_l", dst="id_r", cluster_id_colname="clus_id"
    ).toPandas()

    assert df_result["harmonic_centrality"][0] == pytest.approx(1.50, 0.1)
    assert df_result["harmonic_centrality"][4] == pytest.approx(2.0, 0.1)

    f1 = df_result["node_id"] == "a"
    assert df_result.loc[f1, "clus_id"][0] == 1
