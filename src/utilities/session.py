# coding: utf-8

from pyspark.sql import SparkSession


def new_session():
    """
    Create a spark session

    return : SparkSession
    """
    master = "local"
    appName = "App Name"

    spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    return spark
