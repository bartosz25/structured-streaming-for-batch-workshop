from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from config import get_checkpoint_location, kafka_input_topic

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

input_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", kafka_input_topic()) \
    .option("startingOffsets", "EARLIEST") \
    .load()

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType())
])

query = (input_data.selectExpr("CAST(value AS STRING)")
    .select(F.from_json("value", event_schema).alias("value_struct"), "value")
    .select("value_struct.event_time", "value_struct.event_id", "value")
    .withWatermark("event_time", "10 seconds")
    .groupBy(F.window(F.col("event_time"), "15 seconds")).count()
    .withColumn("current_time", F.current_timestamp()))

writer = query.writeStream.option("checkpointLocation", get_checkpoint_location('window'))\
    .format("console")\
    .option("truncate", False)

writer.start().awaitTermination()