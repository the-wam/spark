from pyspark.sql.functions import lower, col, concat, lit

def drop_nb_visitors_zero_negative(df, column):
    """
    drop all rows with a vistors'number inferior or equal to 0 
    and return a DataFrame

    """

    return df.filter(col(column) > 0)

def to_lowercase(df, column):
    """
    change case to lower and return a DataFrame
    """
    return df.withColumn(column, lower(col(column)))

def split_address(df, column):
    """
    Some rows have two addresses on the column
    we want to split them and create a two rows with one address each
    """

    pass

def add_data_from_address_2(df, column):
    """
    add addresses from second column with address
    """
    pass

def clean_abbreviation(df, column):
    """
    Change asual abbreviation with full word 
    """

    return df.withColumn(column, \
              when(col(column).rlike(" ste "), regexp_replace(col(column), " ste ", " sainte ")) \
              .when(col(column).rlike(" st "), regexp_replace(col(column), " st ", " saint ")) \
              .when(col(column).rlike(" av "), regexp_replace(col(column), " av ", " avenue ")) \
              .when(col(column).rlike(" bld "), regexp_replace(col(column), " bld ", " boulevard ")) \
              .when(col(column).rlike(" crs "), regexp_replace(col(column), " crs ", " cours ")) \
              .otherwise(col(column)))
              
def drop_street_numbers(df, column):
    """
    drop numbers of stress from the address
        (^ *)\d* ?/?-? ?\d* ?,? *
    """

    pass

def clean_hebergement_with_only_avec_sans(df, column):
    """
    uniformise la notation des hebergements
    """

    return df.withColumn(column, \
                when(col(column).rlike("O"), regexp_replace(col(column), "O", "avec")) \
                .when(col(column).rlike("N"), regexp_replace(col(column), "N", "sans")) \
                .when(col(column).rlike(1), regexp_replace(col(column), 1, "avec")) \
                .when(col(column).rlike(0), regexp_replace(col(column), 0, "sans")) \
                .otherwise(col(column)))

def concatenate_address_houssing(df, new_col_name, address_col, houssing_col):
    """
    Concatenate two colomns and create a new one 
    """
    
    return df.withColumn(new_col_name, concat(col(address_col), lit(" "), col(houssing_col)))

