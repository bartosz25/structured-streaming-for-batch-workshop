package com.waitingforcode.exercise3

import com.waitingforcode.InputDataTopicName
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object DecoratorJobWithTriggerAndEnrichment {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/workshop/part02/checkpoint"
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:9094",
        "subscribe" -> InputDataTopicName,
        "startingOffsets" -> "earliest"
      )).load()
      .selectExpr("CAST(value AS STRING) AS value", "NOW() AS current_timestamp")

    val mappedInput = inputStream.selectExpr(
      "value",
      "CONCAT_WS(' >>> ', current_timestamp, value) AS decorated_value"
    )

    val masterDataset = sparkSession.read
      .schema("nr STRING, label STRING")
      .json("/tmp/wfc/workshop/master")

    val enrichedDataset = mappedInput.join(
      masterDataset, masterDataset("nr") === mappedInput("value"), "left"
    )

    val writeQuery = enrichedDataset.writeStream.format("console")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("truncate", false)
      .option("checkpointLocation", checkpointLocation).start()

    writeQuery.awaitTermination()
  }

}