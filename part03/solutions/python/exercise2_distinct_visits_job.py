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

event_schema_for_deduplication = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType())
])

query = input_data.selectExpr("CAST(value AS STRING)") \
    .select(functions.from_json("value", event_schema_for_deduplication).alias("value_struct"), "value") \
    .select("value_struct.event_time", "value_struct.event_id", "value") \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["event_id", "event_time"])\
    .drop("event_time", "event_id") # keep only the value which is required for the sink!

writer = query.writeStream.option("checkpointLocation", get_checkpoint_location('deduplication'))\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9094")\
    .option("topic", kafka_deduped_topic())

writer.start().awaitTermination()
