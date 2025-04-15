from argparse import ArgumentParser
import time
import logging
import psutil
import sys
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import min, max, col


def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument('--data-path', '-d', required=True)
    parser.add_argument('--optimized', '-o', action='store_true')
    return parser.parse_args()


def main(data_path, is_optimized):
    conf = SparkConf()
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.memory", "4g")

    spark = (
        SparkSession.builder.appName("SparkPerformanceApp")
        .master("spark://spark-master:7077")
        .config(conf=conf)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    start_time = time.time()
    df = spark.read.csv(
        data_path, header=True, inferSchema=True
    )
    if is_optimized:
        df = df.repartition(5).cache()

    df.show(5)

    process = psutil.Process(os.getpid())
    ram_usage_mb = process.memory_info().rss / (1024 * 1024)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.data_path, args.optimized)
