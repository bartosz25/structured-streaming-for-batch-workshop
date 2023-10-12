package com.waitingforcode

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Exercise2DistinctVisitsJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()

    val inputDataStream = sparkSession.readStream
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:9094",
        "subscribe" -> InputDataTopicName,
        "startingOffsets" -> "EARLIEST",
      ))
      .format("kafka").load()

    val eventSchema = StructType(Seq(
      StructField("event_id", StringType),
      StructField("event_time", TimestampType)
    ))

    import sparkSession.implicits._
    val query = inputDataStream.selectExpr("CAST(value AS STRING)")
      .select(functions.from_json($"value", eventSchema).alias("value_struct"), $"value")
      .select("value_struct.event_time", "value_struct.event_id", "value")
      .withWatermark("event_time", "10 minutes")
      .dropDuplicates(Seq("event_id", "event_time"))
      // keep only the value which is required for the sink !
      .drop("event_time", "event_id")

    val writer = query.writeStream.option("checkpointLocation", "/tmp/wfc/workshop/part03/checkpoint/deduplication")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("topic", DedupedDataTopicName)

    writer.start().awaitTermination()
  }

}
