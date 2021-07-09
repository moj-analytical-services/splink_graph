from pyspark.sql import Row
from splink_graph.node_metrics import (
    eigencentrality,
    harmoniccentrality
)
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pyspark.sql.functions as f
import json


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


def test_harmoniccentrality_customcolname(spark):
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
