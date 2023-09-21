import datetime
from typing import Iterable

from pyspark import Row
from pyspark.sql import SparkSession, functions, DataFrame

from config import kafka_input_topic

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", kafka_input_topic()) \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()

numbers = input_data_stream.selectExpr("CAST(value AS STRING)", "NOW() AS current_timestamp")


def decorate_numbers(numbers_dataframe: DataFrame, batch_number: int):
    def decorate_number_rows(rows_to_decorate: Iterable[Row]):
        for row in rows_to_decorate:
            yield Row(value=row.value, decorated_value=f'{row.current_timestamp} >>> {row.value}')

    (numbers_dataframe.rdd.mapPartitions(decorate_number_rows).toDF(['value', 'decorated_value'])
     .show(truncate=False))


write_data_stream = numbers.writeStream \
    .foreachBatch(decorate_numbers)

write_query = write_data_stream.start()
write_query.awaitTermination()
