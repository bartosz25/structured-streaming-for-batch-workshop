from pyspark.sql import SparkSession

from config import kafka_input_topic, get_checkpoint_location

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", kafka_input_topic()) \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()

numbers = input_data_stream.selectExpr("CAST(value AS STRING)", "NOW() AS current_timestamp")


def concat_values_with_now(rows):
    for row in rows:
        row["decorated_value"] = row["current_timestamp"].astype(str) + " >>> " + row["value"]
        yield row


mapped_numbers = numbers.mapInPandas(concat_values_with_now, "value STRING, decorated_value STRING, current_timestamp TIMESTAMP")

write_data_stream = (mapped_numbers.writeStream.format("console").option("truncate", False)
                     .option("checkpointLocation", get_checkpoint_location())
                     .queryName("Python API"))

write_query = write_data_stream.start()
write_query.awaitTermination()
