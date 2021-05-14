from pyspark.sql import Row
from splink_graph.splink_graph import _graphharmoniser, subgraph_stats
import pytest
import pandas as pd
import pyspark.sql.functions as f


def test_graphharmoniser(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "component": 2},
    ]

    data_list2 = [
        {"dst": "b", "src": "a", "distance": 0.4, "component": 1},
        {"dst": "c", "src": "b", "distance": 0.56, "component": 1},
        {"dst": "e", "src": "d", "distance": 0.2, "component": 2},
        {"dst": "f", "src": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    h_df = spark.createDataFrame(Row(**x) for x in data_list2)

    harmonised = _graphharmoniser(e_df, "src", "dst")

    df_result = harmonised.toPandas().reindex(
        ["component", "distance", "src", "dst"], axis=1
    )
    df_expected = h_df.toPandas().reindex(
        ["component", "distance", "src", "dst"], axis=1
    )

    pd.testing.assert_frame_equal(df_result, df_expected)


def test_subgraph_stats(spark):
    # Create an Edge DataFrame with "src" and "dst" columns
    data_list = [
        {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
        {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
        {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
        {"src": "f", "dst": "e", "distance": 0.8, "component": 2},
    ]

    e_df = spark.createDataFrame(Row(**x) for x in data_list)
    e_df = e_df.withColumn("weight", 1.0 - f.col("distance"))
    df_result = subgraph_stats(
        e_df, component="component", weight="weight", src="src", dst="dst"
    ).toPandas()

    assert (df_result["nodecount"] == 3).all()
    assert (df_result["edgecount"] == 2).all()
    assert df_result["density"].values == pytest.approx(0.666667, 0.01)
