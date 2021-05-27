# coding: utf-8

import sys

sys.path.insert(0, "./src")

from utilities.session import new_session
from utilities.manipulate_data import import_csv, to_parquet, save_csv
from cleaning.cleaning import cleainng_order
from group.group_data import group_ordered


def main():
    # create session
    spark = new_session()

    # path
    path_to_original_csv_file = "./bor_erp.csv"
    path_to_parquet_file = "./data/parquet/bor_erp.parquet"
    path_to_final_result = "./data/csv/final.csv"

    # import data
    df = import_csv(spark, path_to_original_csv_file)

    # create a parquet file with options
    partition_by = "type"
    data_parquet = to_parquet(spark, df, partition_by, path_to_parquet_file)

    # cleaning
    data_parquet = cleainng_order(data_parquet)

    # group data
    data_parquet = group_ordered(data_parquet)

    # save as csv
    save_csv(data_parquet, path_to_final_result)


if __name__ == "__main__":
    main()
