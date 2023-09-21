package com.waitingforcode.exercise2

import com.waitingforcode.InputDataTopicName
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object DecoratorJobWithTrigger {

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
      "CONCAT_WS('', current_timestamp, value) AS decorated_value"
    )

    val writeQuery = mappedInput.writeStream.format("console")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("truncate", false)
      .option("checkpointLocation", checkpointLocation).start()

    writeQuery.awaitTermination()
  }

}