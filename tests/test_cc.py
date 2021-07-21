from pyspark.sql import Row
import pyspark
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pyspark.sql.functions as f
from splink_graph.cc import _find_graphframes_jars, sp_connected_components
from graphframes import GraphFrame


@pytest.mark.order(1)
def test_cc_simple(sparkwithgraphframes, graphframes_tmpdir):

    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = sparkwithgraphframes.createDataFrame(
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

    assert _find_graphframes_jars(sparkwithgraphframes) == 0

    sparkwithgraphframes.sparkContext.setCheckpointDir("graphframes_tempdir/")
    df_result = sp_connected_components(
        e2_df, src="src", dst="dst", weight_colname="weight"
    ).toPandas()

    assert df_result["cluster_id"].count() == 2
    assert df_result["node_id"].unique().size == 2

    df_result2 = sp_connected_components(
        e2_df, src="src", dst="dst", weight_colname="weight", cc_threshold=0.2
    ).toPandas()
    assert df_result2["cluster_id"].unique().size == 2
    assert df_result2["node_id"].count() == 10
