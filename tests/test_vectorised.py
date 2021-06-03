from pyspark.sql import Row
from splink_graph.vectorised import (
    diameter_radius_transitivity,
    edgebetweeness,
    eigencentrality,
    harmoniccentrality,
)
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pyspark.sql.functions as f


def test_diameter_radius_transitivity(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = diameter_radius_transitivity(e_df).toPandas()

    assert df_result["diameter"][0] == 2
    assert df_result["diameter"][1] == 2

    assert df_result["radius"][0] == 1
    assert df_result["radius"][1] == 1

    assert df_result["transitivity"][0] == pytest.approx(0, 0.01)
    assert df_result["transitivity"][1] == pytest.approx(0, 0.01)

    assert df_result["graphhash"][0] == "0f43d8cdd43b0b78727b192b6d6d0d0e"
    assert df_result["graphhash"][1] == "0f43d8cdd43b0b78727b192b6d6d0d0e"


def test_diameter_radius_transitivity_customcolname(spark):
    # Create an Edge DataFrame with "id_l" and "id_r" columns
    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "component": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "component": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "component": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = diameter_radius_transitivity(e_df, src="id_l", dst="id_r").toPandas()

    assert df_result["diameter"][0] == 2
    assert df_result["diameter"][1] == 2


def test_edgebetweeness_samecomponentsinout(spark):
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
        {"src": "e", "dst": "f", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = edgebetweeness(e_df).toPandas()

    assert_frame_equal(
        e_df.toPandas()
        .drop(["weight", "distance"], axis=1)
        .reindex(["src", "dst", "component"], axis=1),
        df_result.drop("eb", axis=1).sort_values("component").reset_index(drop=True),
    )


def test_edgebetweeness_simple(spark):
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = edgebetweeness(e_df).toPandas()

    assert df_result["eb"].values == pytest.approx(0.666667, 0.1)


def test_edgebetweeness_simple_customcolname(spark):
    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "component": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "component": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "component": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = edgebetweeness(e_df, src="id_l", dst="id_r").toPandas()

    assert df_result["eb"].values == pytest.approx(0.666667, 0.1)


def test_eigencentrality_simple(spark):
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = eigencentrality(e_df).toPandas()

    assert df_result["eigen_centrality"][0] == pytest.approx(0.50000, 0.01)


def test_eigencentrality_customcolname(spark):

    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "component": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "component": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "component": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = eigencentrality(e_df, src="id_l", dst="id_r").toPandas()

    assert df_result["eigen_centrality"][0] == pytest.approx(0.50000, 0.01)


def test_harmoniccentrality_simple(spark):
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = harmoniccentrality(e_df).toPandas()

    assert df_result["harmonic_centrality"][0] == pytest.approx(1.50, 0.1)
    assert df_result["harmonic_centrality"][4] == pytest.approx(2.0, 0.1)


def test_harmoniccentrality_customcolname(spark, src="id_l", dst="id_r"):
    data_list = [
        {"id_l": "a", "id_r": "b", "distance": 0.4, "component": 1},
        {"id_l": "b", "id_r": "c", "distance": 0.56, "component": 1},
        {"id_l": "d", "id_r": "e", "distance": 0.2, "component": 2},
        {"id_l": "f", "id_r": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))

    df_result = harmoniccentrality(e_df, src="id_l", dst="id_r").toPandas()

    assert df_result["harmonic_centrality"][0] == pytest.approx(1.50, 0.1)
    assert df_result["harmonic_centrality"][4] == pytest.approx(2.0, 0.1)
