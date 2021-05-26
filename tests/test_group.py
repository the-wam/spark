# coding: utf-8

from src.group.group_data import concatenate_address_hebergement, group_by_hebergement_and_adress
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
from tests.order_pandas import get_sorted_data_frame

import pytest

@pytest.mark.group
def test_concatenate_address_hebergement():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    input_df = sql_context.createDataFrame(
        [("rue charles", "avec"),
         ("avenue fabien", "sans"),
         ("allée sam", "sans"),
         ("avenue fabien", "sans"),
         ("quai nick", "avec"),
         ("quai nick", "sans")],
         ["name", "hebergement"]
    )

    expected_df = sql_context.createDataFrame(
        [("rue charles", "avec", "rue charles avec"),
         ("avenue fabien", "sans", "avenue fabien sans"),
         ("allée sam", "sans", "allée sam sans"),
         ("avenue fabien", "sans", "avenue fabien sans"),
         ("quai nick", "avec", "quai nick avec"),
         ("quai nick", "sans", "quai nick sans")],
         ["name", "hebergement", "concat"]
    )
    

    real_df = concatenate_address_hebergement(input_df, "concat", "name", "hebergement")
    real_df = get_sorted_data_frame(
        real_df.toPandas(),
        ["concat", "name", "hebergement"]
    )
    expected_df = get_sorted_data_frame(
        expected_df.toPandas(),
        ["concat", "name", "hebergement"]
    )

    # Equality assertion
    pd.testing.assert_series_equal(
        expected_df["concat"],
        real_df["concat"]
    )

    # Close the Spark Context
    spark_context.stop()

@pytest.mark.group
def test_group_by_hebergement_and_adress():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    input_df = sql_context.createDataFrame(
        [(10, "rue charles avec"),
         (34, "avenue fabien sans"),
         (4, "quai nick sans"),
         (8, "avenue fabien sans"),
         (23, "quai nick avec"),
         (56, "quai nick sans")],
         ["visitors", "visitors_hebergement"]
    )

    expected_df = sql_context.createDataFrame(
        [(10, "rue charles avec"),
         (42, "avenue fabien sans"),
         (60, "quai nick sans"),
         (23, "quai nick avec")],
         ["visitors", "visitors_hebergement"]
    )
    

    real_df = group_by_hebergement_and_adress(input_df, "visitors_hebergement", "visitors")
    real_df = get_sorted_data_frame(
        real_df.toPandas(),
        ["visitors", "visitors_hebergement"]
    )
    expected_df = get_sorted_data_frame(
        expected_df.toPandas(),
        ["visitors", "visitors_hebergement"]
    )

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_df,
        real_df,
        check_like=True,
    )

    # Close the Spark Context
    spark_context.stop()
