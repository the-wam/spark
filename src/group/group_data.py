from pyspark.sql.functions import col, concat, lit


def concatenate_address_hebergement(df, new_col_name = "street_hebergement", address_col = "adresse_1", hebergement_col = "avec_hebergement"):
    """
    Concatenate two colomns and create a new one 
    and return a DataFrame
    """
    
    return df.withColumn(new_col_name, concat(col(address_col), lit(" "), col(hebergement_col)))

def group_by_hebergement_and_adress(df, column1 = "street_hebergement", column2 = "nb_visiteurs_max"):
    """
    Grou
    and return a DataFrame
    """

    return df.select(column1, column2) \
            .groupBy(col(column1)).sum() \
            .orderBy(col(column1).desc()) \
            .withColumnRenamed(f"sum({column2})", column2).orderBy(col("nb_visiteurs_max").desc())


def final_group(df):
    """

    """

    df = concatenate_address_hebergement(df)
    df = group_by_hebergement_and_adress(df)

    return df