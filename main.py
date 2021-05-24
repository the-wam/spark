import os
import sys

sys.path.insert(0, "./src")

from utilities.session import new_session
from utilities.manipulate_data import import_csv, to_parquet
from cleaning.cleaning import *
from group.group_data import group_by_hebergement_and_adress

def main():
    # create session 
    spark = new_session()
    
    # import data
    df = import_csv(spark, "./bor_erp.csv")
    
    # create a parquet file with options
    partition_by = "type"
    path_parquet_file = "./data/bor_erp.parquet"
    data_parquet = to_parquet(spark, df, partition_by, path_parquet_file)

    data_parquet.printSchema()

    # cleaning
    step1 = drop_nb_visitors_zero_negative(data_parquet, "nb_visiteurs_max")
    step2 = to_lowercase(step1, "adresse_1")
    step3 = 

if __name__ == '__main__':
    main()
