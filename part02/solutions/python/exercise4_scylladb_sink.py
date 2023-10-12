from pyspark.sql import SparkSession

from config import kafka_input_topic, get_checkpoint_location
from scylladb_writer import ScyllaDbWriter

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", kafka_input_topic()) \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()

numbers = input_data_stream.selectExpr("CAST(value AS STRING) AS value", "NOW() AS current_timestamp")

mapped_input = numbers.selectExpr("value",
                                  "CONCAT_WS(' >>>> ', current_timestamp, value) AS decorated_value")

master_dataset = spark.read.schema("nr STRING, label STRING").json("/tmp/wfc/workshop/master")

enriched_dataset = mapped_input.join(master_dataset, [mapped_input.value == master_dataset.nr],
                                     "left")

write_data_stream = (enriched_dataset.writeStream
                     .trigger(processingTime="30 seconds")
                     .option("checkpointLocation", get_checkpoint_location())
                     .foreach(ScyllaDbWriter())
                     .queryName("SQL API"))

write_query = write_data_stream.start()
write_query.awaitTermination()
