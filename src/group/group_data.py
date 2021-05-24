from pyspark.sql.functions import col

def group_by_hebergement_and_adress(df, column):
    """

    """

    return df.groubBy(col(column)).sum().orderBy(col(column).desc())