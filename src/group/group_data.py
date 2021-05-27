# coding: utf-8

from pyspark.sql.functions import col, concat, lit, split


def concatenate_address_hebergement(
    df,
    new_col_name="street_hebergement",
    address_col="adresse_1",
    hebergement_col="avec_hebergement",
):
    """
    Concatenate two colomns and create a new one
    and return a DataFrame
    """

    return df.withColumn(
        new_col_name, concat(col(address_col), lit("!"), col(hebergement_col))
    )


def group_by_hebergement_and_address(
    df,
    old_col_h_a="street_hebergement",
    new_col_address="adresse",
    new_col_hebergement="hebergement",
    nb_visitors="nb_visiteurs_max",
):
    """
    Group data by hebergement and sum the number of visitors
    and return a DataFrame
    """
    split_col = split(df[old_col_h_a], "!")

    return (
        df.select(old_col_h_a, nb_visitors)
        .groupBy(col(old_col_h_a))
        .sum()
        .orderBy(col(old_col_h_a).desc())
        .withColumnRenamed(f"sum({nb_visitors})", nb_visitors)
        .orderBy(col(nb_visitors).desc())
        .withColumn(new_col_address, split_col.getItem(0))
        .withColumn(new_col_hebergement, split_col.getItem(1))
        .drop(old_col_h_a)
    )


def group_ordered(df):
    """
    Generate the final file
    """

    df = concatenate_address_hebergement(df)
    df = group_by_hebergement_and_address(df)

    return df
