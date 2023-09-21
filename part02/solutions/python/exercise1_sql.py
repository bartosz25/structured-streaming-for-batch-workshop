from pyspark.sql import SparkSession

from config import kafka_input_topic, get_checkpoint_location

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
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

write_data_stream = (mapped_input.writeStream.format("console")
                     .option("truncate", False)
                     .option("checkpointLocation", get_checkpoint_location())
                     .queryName("SQL API"))

write_query = write_data_stream.start()
write_query.awaitTermination()
