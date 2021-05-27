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

    # import data
    df = import_csv(spark, "./bor_erp.csv")

    # create a parquet file with options
    partition_by = "type"
    path_parquet_file = "./data/parquet/bor_erp.parquet"
    data_parquet = to_parquet(spark, df, partition_by, path_parquet_file)

    # cleaning
    data_parquet = cleainng_order(data_parquet)

    # group data
    data_parquet = group_ordered(data_parquet)

    # save as csv
    save_csv(data_parquet, "./data/csv/final.csv")


if __name__ == "__main__":
    main()
