from pyspark.sql import Row
import pyspark
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import pyspark.sql.functions as f
from splink_graph.cc import _find_graphframes_jars,sp_connected_components
from graphframes import GraphFrame

def test_cc_simple(sparkwithgraphframes):
    # Create an Edge DataFrame with "id_l" and "id_r" columns
    
    # Create an Edge DataFrame with "src" and "dst" columns
    e2_df = sparkwithgraphframes.createDataFrame(
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
        ["src", "dst", "weight"])
    
    
    
    assert _find_graphframes_jars(sparkwithgraphframes) == 0
    
    #df_result =  sp_connected_components(e2_df,src="src",dst="dst",weight_colname="weight")
    
    