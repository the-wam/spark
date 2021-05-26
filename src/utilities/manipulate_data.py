import os
import glob

def import_csv(spark, path, infer_schema = "true", header = "true", sep = ";"):
    """
    import a csv file
    and return a dataframe
    """
    file_location = path
    file_type = "csv"

    df = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", header) \
            .option("sep", sep) \
            .load(file_location)

    return df


def save_csv(df, path):
    """
    By CAREFUL with the size of the data this function use COALESCE
    Save as .csv
    """
    df.coalesce(1).write.csv(path)

def write_parquet(df, partition_by, path):
    """
    Write a parquet file 

    """

    df.write.mode("overwrite").partitionBy(partition_by).parquet(path)


def read_parquet(spark, path):
    """
    Read a parquet file 
    and return a dataframe
    """

    return spark.read.parquet(path)


def to_parquet(spark, df, partition_by, path):
    """
    Write then read a parquet file
    and return a dataframe
    """
    write_parquet(df, partition_by, path)

    return read_parquet(spark, path)

