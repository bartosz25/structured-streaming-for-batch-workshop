package com.waitingforcode.exercise1

import com.waitingforcode.{InputDataTopicName, MappedEvent}
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

object DecoratorJobScalaApi {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/workshop/part02/checkpoint"
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> InputDataTopicName,
        "startingOffsets" -> "earliest"
      )).load()
      .selectExpr("CAST(value AS STRING) AS value", "NOW() AS current_timestamp").as[(String, LocalDateTime)]

    val mappedInput = inputStream.map(valueWithNow => MappedEvent(
      value = valueWithNow._1,
      decorated_value = s"${valueWithNow._2} >>> ${valueWithNow._1}")
    )

    val writeQuery = mappedInput.writeStream.format("console")
      .option("truncate", false)
      .option("checkpointLocation", checkpointLocation)
      .queryName("Scala API").start()

    writeQuery.awaitTermination()
  }

}

