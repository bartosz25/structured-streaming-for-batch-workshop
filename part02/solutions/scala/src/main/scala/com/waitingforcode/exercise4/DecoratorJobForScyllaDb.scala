package com.waitingforcode.exercise4

import com.waitingforcode.{InputDataTopicName, MappedEventWithLabel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object DecoratorJobForScyllaDb {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/workshop/part02/checkpoint"
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:9094",
        "subscribe" -> InputDataTopicName,
        "startingOffsets" -> "earliest"
      )).load()
      .selectExpr("CAST(value AS STRING) AS value", "NOW() AS current_timestamp")

    val mappedInput = inputStream.selectExpr(
      "value",
      "CONCAT_WS('', current_timestamp, value) AS decorated_value",
      "label"
    ).as[MappedEventWithLabel]

    val writeQuery = mappedInput.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", checkpointLocation)
      .foreach(new ScyllaDbWriter())
      .start()

    writeQuery.awaitTermination()
  }

}