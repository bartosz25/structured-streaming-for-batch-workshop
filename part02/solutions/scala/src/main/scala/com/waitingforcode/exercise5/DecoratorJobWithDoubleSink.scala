package com.waitingforcode.exercise5

import com.waitingforcode.{EnrichedDataTopicName, InputDataTopicName}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DecoratorJobWithDoubleSink {

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

    def writeDatasetToKafkaAndJson(dataset: DataFrame, batchNumber: Long): Unit = {
      // Cache is very important! TODO: show on the UI what happens if it's not there
      val datasetToWrite = dataset.cache()
      datasetToWrite.write.mode(SaveMode.Overwrite).json(s"/tmp/wfc/workshop/part02/exercise4/${batchNumber}")
      // Kafka is not only available for the streaming API!
      datasetToWrite.selectExpr("decorated_value AS value").write.options(Map(
        "kafka.bootstrap.servers" -> "localhost:9094",
        "topic" -> EnrichedDataTopicName
      )).format("kafka").save()
      datasetToWrite.unpersist()
      ()
    }

    val writeQuery = mappedInput.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch(writeDatasetToKafkaAndJson _)
      .start()

    writeQuery.awaitTermination()
  }

}