
def import_csv(spark, path, infer_schema = "true", header = "true", sep = ";"):
    """
        
    """
    file_location = path
    file_type = "csv"

    df = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", header) \
            .option("sep", sep) \
            .load(file_location)

    return df

def write_parquet(df, partition_by, path):
    """
    write a parquet file 

    """

    df.write.mode("overwrite").partitionBy(partition_by).parquet(path)


def read_parquet(spark, path):
    """
    read a parquet file 

    return 
    """

    return spark.read.parquet(path)

def to_parquet(spark, df, partition_by, path):
    """

    """
    write_parquet(df, partition_by, path)

    return read_parquet(spark, path)