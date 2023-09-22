import datetime
from typing import Iterable

from pyspark import Row
from pyspark.sql import SparkSession, functions, DataFrame

from config import kafka_input_topic

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
    .getOrCreate()
