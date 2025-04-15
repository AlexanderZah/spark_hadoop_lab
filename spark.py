# coding: utf-8
import time
import logging
import psutil
import sys
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import min, max, col

OPTIMIZED = True if sys.argv[1] == "Optimized" else False



conf = SparkConf()
conf.set("spark.ui.showConsoleProgress", "false")
conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
conf.set("spark.executor.memory", "1g")
conf.set("spark.driver.memory", "2g")


spark = (
    SparkSession.builder.appName("SparkPerformanceApp")
    .master("spark://spark-master:7077")
    .config(conf=conf)
    .getOrCreate()
)


spark.sparkContext.setLogLevel("ERROR")


start_time = time.time()



df = spark.read.csv(
    "hdfs:///data/steambans.csv", header=True, inferSchema=True
)




if OPTIMIZED:
    df = df.repartition(5).cache()


df.show(5)


process = psutil.Process(os.getpid())
ram_usage_mb = process.memory_info().rss / (1024 * 1024)




