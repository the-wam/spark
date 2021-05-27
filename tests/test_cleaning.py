# coding: utf-8

from src.cleaning.cleaning import to_lowercase, drop_street_numbers
from src.cleaning.cleaning import union_addresses_columns
from src.cleaning.cleaning import clean_hebergement_with_only_avec_sans
from src.cleaning.cleaning import cleaning_addresses_with_geometrie
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
from tests.order_pandas import get_sorted_data_frame

import pytest


@pytest.mark.cleaning
def test_to_lowercase():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    input_df = sql_context.createDataFrame(
        [
            ("chArly", 15),
            ("fabien", 18),
            ("saM", 21),
            ("sAm", 25),
            ("niCk", 19),
            ("Nick", 40),
        ],
        ["name", "age"],
    )

    expected_df = sql_context.createDataFrame(
        [
            ("charly", 15),
            ("fabien", 18),
            ("sam", 21),
            ("sam", 25),
            ("nick", 19),
            ("nick", 40),
        ],
        ["name", "age"],
    )

    real_df = to_lowercase(input_df, "name")
    real_df = get_sorted_data_frame(
        real_df.toPandas(),
        ["age", "name"],
    )
    expected_df = get_sorted_data_frame(
        expected_df.toPandas(),
        ["age", "name"],
    )

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_df,
        real_df,
        check_like=True,
    )

    # Close the Spark Context
    spark_context.stop()


@pytest.mark.cleaning
def test_drop_street_numbers():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    input_df = sql_context.createDataFrame(
        [
            (1, "3 - 04 boulevard du Président Wilson"),
            (2, "13bis rue Montbazon"),
            (3, "49/3 AV MAL DE LATTRE DE TASSIGNY"),
            (4, "26, rue Rolland"),
            (5, "                        9, avenue Raymond Poincaré"),
            (6, "  11-17 rue Condillac"),
            (7, "  place de la Bourse"),
            (8, "39 à 57 RUE BOUFFARD"),
            (9, "26-3  COURS DE LA SOMME"),
            (10, "26-3  RUE DE MARMANDE"),
            (11, "262 ter COURS DE LA SUM"),
            (12, "rue Corps Franc Pommies"),
        ],
        ["index", "address"],
    )

    expected_df = sql_context.createDataFrame(
        [
            (1, "boulevard du Président Wilson"),
            (2, "rue Montbazon"),
            (3, "AV MAL DE LATTRE DE TASSIGNY"),
            (4, "rue Rolland"),
            (5, "avenue Raymond Poincaré"),
            (6, "rue Condillac"),
            (7, "place de la Bourse"),
            (8, "RUE BOUFFARD"),
            (9, "COURS DE LA SOMME"),
            (10, "RUE DE MARMANDE"),
            (11, "COURS DE LA SUM"),
            (12, "rue Corps Franc Pommies"),
        ],
        ["index", "address"],
    )

    real_df = drop_street_numbers(input_df, "address")
    real_df = get_sorted_data_frame(
        real_df.toPandas(),
        ["index", "address"],
    )
    expected_df = get_sorted_data_frame(
        expected_df.toPandas(),
        ["index", "address"],
    )

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_df,
        real_df,
        check_like=True,
    )

    # Close the Spark Context
    spark_context.stop()


@pytest.mark.cleaning
def test_clean_hebergement_with_only_avec_sans():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    input_df = sql_context.createDataFrame(
        [
            ("N", "boulevard du Président Wilson"),
            ("1", "rue Montbazon"),
            ("0", "AV MAL DE LATTRE DE TASSIGNY"),
            ("0", "rue Rolland"),
            ("1", "avenue Raymond Poincaré"),
            ("O", "rue Condillac"),
            ("N", "place de la Bourse"),
            ("1", "RUE BOUFFARD"),
            ("1", "COURS DE LA SOMME / RUE DE MARMANDE"),
            ("O", "rue Corps Franc Pommies"),
        ],
        ["hebergement", "address"],
    )

    expected_df = sql_context.createDataFrame(
        [
            ("sans", "boulevard du Président Wilson"),
            ("avec", "rue Montbazon"),
            ("sans", "AV MAL DE LATTRE DE TASSIGNY"),
            ("sans", "rue Rolland"),
            ("avec", "avenue Raymond Poincaré"),
            ("avec", "rue Condillac"),
            ("sans", "place de la Bourse"),
            ("avec", "RUE BOUFFARD"),
            ("avec", "COURS DE LA SOMME / RUE DE MARMANDE"),
            ("avec", "rue Corps Franc Pommies"),
        ],
        ["hebergement", "address"],
    )

    real_df = clean_hebergement_with_only_avec_sans(input_df, "hebergement")
    real_df = get_sorted_data_frame(
        real_df.toPandas(),
        ["hebergement", "address"],
    )
    expected_df = get_sorted_data_frame(
        expected_df.toPandas(),
        ["hebergement", "address"],
    )

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_df,
        real_df,
        check_like=True,
    )

    # Close the Spark Context
    spark_context.stop()


@pytest.mark.cleaning
def test_union_addresses_columns():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    input_df = sql_context.createDataFrame(
        [
            (1, "boulevard du Président Wilson", "RUE BOUFFARD"),
            (2, "rue Montbazon", None),
            (3, "AV MAL DE LATTRE DE TASSIGNY", None),
            (4, "rue Rolland", "COURS DE LA SOMME"),
        ],
        ["row_number", "address_1", "address_2"],
    )

    expected_df = sql_context.createDataFrame(
        [
            (1, "boulevard du Président Wilson"),
            (2, "rue Montbazon"),
            (3, "AV MAL DE LATTRE DE TASSIGNY"),
            (4, "rue Rolland"),
            (1, "RUE BOUFFARD"),
            (4, "COURS DE LA SOMME"),
        ],
        ["row_number", "address_1"],
    )

    real_df = union_addresses_columns(input_df, "address_1", "address_2")
    real_df = get_sorted_data_frame(
        real_df.toPandas(),
        ["row_number", "address_1"],
    )
    expected_df = get_sorted_data_frame(
        expected_df.toPandas(),
        ["row_number", "address_1"],
    )

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_df,
        real_df,
        check_like=True,
    )

    # Close the Spark Context
    spark_context.stop()


@pytest.mark.cleaning
def test_cleaning_addresses_with_geometrie():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    input_df = sql_context.createDataFrame(
        [
            ("44.88067,-0.570246", "Loge N° 41", "C.C.AUCHAN LAC : MINEL"),
            ("44.88067,-0.570246", "Loge N° 147", "C.C.AUCHAN LAC: FNAC"),
            ("44.88067,-0.570246", None, "C.C.AUCHAN LAC: TRESOR"),
            ("44.88067,-0.570242", "Loge N°134", "C.C.AUCHAN LAC: PROVOST"),
            ("44.88067,-0.570241", None, "C.C.AUCHAN LAC : JULES"),
        ],
        ["geometrie", "address_1", "name"],
    )

    expected_df = sql_context.createDataFrame(
        [
            ("44.88067,-0.570246", "rue de la paix", "C.C.AUCHAN LAC : MINEL"),
            ("44.88067,-0.570246", "rue de la paix", "C.C.AUCHAN LAC: FNAC"),
            ("44.88067,-0.570246", "rue de la paix", "C.C.AUCHAN LAC: TRESOR"),
            ("44.88067,-0.570242", "Loge N°134", "C.C.AUCHAN LAC: PROVOST"),
            ("44.88067,-0.570241", None, "C.C.AUCHAN LAC : JULES"),
        ],
        ["geometrie", "address_1", "name"],
    )

    real_df = cleaning_addresses_with_geometrie(
        input_df, "address_1", "44.88067,-0.570246", "rue de la paix"
    )
    real_df = get_sorted_data_frame(
        real_df.toPandas(), ["geometrie", "address_1", "name"]
    )
    expected_df = get_sorted_data_frame(
        expected_df.toPandas(), ["geometrie", "address_1", "name"]
    )

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_df,
        real_df,
        check_like=True,
    )

    # Close the Spark Context
    spark_context.stop()
