from pyspark.sql import Row
from splink_graph.utils import _graphharmoniser, _assert_columns
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


# test passes if there is an exception thrown by _assert_columns
def test_assert_columns(spark):
    with pytest.raises(Exception):
        # Create an Edge DataFrame with "src" and "dst" columns
        data_list = [
            {"src": "a", "dst": "b", "distance": 0.4, "component": 1},
            {"src": "b", "dst": "c", "distance": 0.56, "component": 1},
            {"src": "d", "dst": "e", "distance": 0.2, "component": 2},
            {"src": "f", "dst": "e", "distance": 0.8, "component": 2},
        ]

        e_df = spark.createDataFrame(Row(**x) for x in data_list)

        _assert_columns(e_df, ["src", "dst", "lol"])
