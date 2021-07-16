from pyspark.sql import Row

from splink_graph.cluster_metrics import cluster_main_stats,cluster_basic_stats

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
        e_df, src="src", dst="dst",cluster_id_colname="cluster_id",weight_colname="weight",
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

    df_result = cluster_main_stats(e_df, src="id_l", dst="id_r",cluster_id_colname="estimated_id").toPandas()

    assert df_result["diameter"][0] == 2
    assert df_result["diameter"][1] == 2    
    