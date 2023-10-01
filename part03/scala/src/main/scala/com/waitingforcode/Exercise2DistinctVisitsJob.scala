package com.waitingforcode

import org.apache.spark.sql.SparkSession

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

    import sparkSession.implicits._
    val query = inputDataStream.selectExpr("CAST(value AS STRING)")

    val writer = query.writeStream.option("checkpointLocation", "/tmp/wfc/workshop/part03/checkpoint/deduplication")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("topic", DedupedDataTopicName)

    writer.start().awaitTermination()
  }

}
