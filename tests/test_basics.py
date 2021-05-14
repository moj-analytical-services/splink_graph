import pytest


def test_spark_exists(spark):

    assert spark.version
