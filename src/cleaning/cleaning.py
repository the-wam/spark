# coding: utf-8

from pyspark.sql.functions import lower, col, regexp_replace, when, lit
from pyspark.sql.types import StringType


def check_columns(df):
    """
    Check if the file have the same columns and return Boolean
    """
    right_columns = [
        "avec_hebergement",
        "nom_etablissement",
        "propriete_ville",
        "categorie",
        "adresse_1",
        "adresse_2",
        "code_postal",
        "commune",
        "canton",
        "effectif_personnel",
        "nb_visiteurs_max",
        "geometrie",
        "type",
    ]
    return df.columns == right_columns


def cleaning_addresses_with_geometrie(df, column, geometrie, new_adress):
    """
    cleaning some addresses with them locatisation
    and return a dataframe
    """

    return df.withColumn(
        column, when(col("geometrie") == geometrie, new_adress).otherwise(col(column))
    )


def list_cleaning_addresses_with_geometrie(df):
    """
    Use cleaning_addresses_with_geometrie
    and return a DataFrame
    """
    df = cleaning_addresses_with_geometrie(
        df, "adresse_1", "44.88067,-0.570246", "avenue des quarante journaux"
    )
    df = cleaning_addresses_with_geometrie(
        df, "adresse_2", "44.88067,-0.570246", lit(None).cast(StringType())
    )
    df = cleaning_addresses_with_geometrie(
        df, "adresse_1", "44.872645,-0.571176", "avenue jean gabriel domergue"
    )
    df = cleaning_addresses_with_geometrie(
        df, "adresse_1", "44.839286,-0.576552", "rue pere louis de jabrun"
    )

    return df


def drop_nb_visitors_zero_negative(df, column="nb_visiteurs_max"):
    """
    Drop all the rows with a vistors number inferior or equal to 0
    and return a DataFrame
    """

    return df.filter(col(column) > 0)


def union_addresses_columns(df, column1="adresse_1", column2="adresse_2"):
    """
    Create a new row with the address from the column adresse_2
    and return a DataFrame
    """
    df_address_2_not_null = (
        df.where(col(column2).isNotNull())
        .drop(column1)
        .withColumnRenamed(column2, column1)
    )

    return df.drop(column2).union(df_address_2_not_null)


def to_lowercase(df, column="adresse_1"):
    """
    Replace capitals by lowercase and return a DataFrame
    """
    return df.withColumn(column, lower(col(column)))


def drop_address_null(df, column="adresse_1"):
    """
    drop the addresses is null
    """
    return df.filter(col(column).isNotNull())


def split_address(df, column="adresse_1"):
    """
    Some rows have two addresses in the same cell.
    We want to split them into two rows, each one with one address
    and return a DataFrame
    """
    # to do

    return df


def clean_abbreviation(df, column="adresse_1"):
    """
    Replace usual abbreviations by full words
    and return a DataFrame
    """

    return df.withColumn(
        column,
        when(
            col(column).rlike(" ave "), regexp_replace(col(column), " ave ", " avenue ")
        )
        .when(
            col(column).rlike(" ste "), regexp_replace(col(column), " ste ", " sainte ")
        )
        .when(col(column).rlike(" st "), regexp_replace(col(column), " st ", " saint "))
        .when(
            col(column).rlike(" av "), regexp_replace(col(column), " av ", " avenue ")
        )
        .when(
            col(column).rlike(" bld "),
            regexp_replace(col(column), " bld ", " boulevard "),
        )
        .when(
            col(column).rlike(" crs "), regexp_replace(col(column), " crs ", " cours ")
        )
        .otherwise(col(column)),
    )


def drop_duplicate(df, columns_list):
    """
    drop duplicate
    """

    return df.dropDuplicates(columns_list)


def drop_street_numbers(df, column="adresse_1"):
    """
    Drop the numbers of streets from the addresses
    and return a DataFrame
    """
    return df.withColumn(
        column,
        regexp_replace(
            col(column), r"(^ *)\d* ?/?-?à? ?\d* ?,? *(bis)?(ter)?(b )?(t )?,? *", ""
        ),
    )


def clean_hebergement_with_only_avec_sans(df, column="avec_hebergement"):
    """
    Standardize hebergement column values
    and return a DataFrame
    """

    return df.withColumn(
        column,
        when(col(column).rlike("O"), regexp_replace(col(column), "O", "avec"))
        .when(col(column).rlike("N"), regexp_replace(col(column), "N", "sans"))
        .when(col(column).rlike("1"), regexp_replace(col(column), "1", "avec"))
        .when(col(column).rlike("0"), regexp_replace(col(column), "0", "sans"))
        .otherwise(col(column)),
    )


def cleainng_order(df):
    """
    Check if the right is using and create the function order to clean it
    """

    if check_columns(df):
        df = drop_nb_visitors_zero_negative(df)
        df = list_cleaning_addresses_with_geometrie(df)
        df = union_addresses_columns(df)
        df = drop_address_null(df)
        df = drop_duplicate(df, ["geometrie", "nb_visiteurs_max"])
        df = drop_duplicate(df, ["adresse_1", "nb_visiteurs_max"])
        df = to_lowercase(df)
        df = clean_abbreviation(df)
        df = drop_street_numbers(df)
        df = clean_hebergement_with_only_avec_sans(df)
    else:
        print("Are you use the right file ?")
        return 0

    return df
