from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from config import get_checkpoint_location, kafka_input_topic, kafka_deduped_topic

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

input_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", kafka_input_topic()) \
    .option("startingOffsets", "EARLIEST") \
    .load()

query = input_data.selectExpr("CAST(value AS STRING)")

writer = query.writeStream.option("checkpointLocation", get_checkpoint_location('deduplication'))\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9094")\
    .option("topic", kafka_deduped_topic())

writer.start().awaitTermination()
