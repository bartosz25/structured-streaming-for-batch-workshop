package com.waitingforcode

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Exercise1WindowJob {

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
    val windowCount = ...

    val writer = windowCount.writeStream
      .option("checkpointLocation", "/tmp/wfc/workshop/part03/checkpoint/window")
      .format("console").option("truncate", false)

    writer.start().awaitTermination()
  }

}
