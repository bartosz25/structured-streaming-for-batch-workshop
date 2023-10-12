package com.waitingforcode

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.sys.process.Process

object DataGenerator {

  def main(args: Array[String]): Unit = {
    Seq(InputDataTopicName, DedupedDataTopicName).foreach(topicToRecreate => {
      println(s"== Deleting already existing topic ${topicToRecreate} ==")
      val deleteTopicResult =
        Process(s"docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic ${topicToRecreate} --delete").run()
      deleteTopicResult.exitValue()
      println(s"== Creating ${topicToRecreate} ==")
      val createTopicResult =
        Process(s"docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic ${topicToRecreate} --create --partitions 2").run()
      createTopicResult.exitValue()
    })
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    import sparkSession.implicits._

    while (true) {
      println(">>> Generating records")
      def generateVisit(visitId: Int): Visit = {
        Visit(event_id = UUID.randomUUID().toString, visit_id = visitId,
          event_time = Timestamp.from(ZonedDateTime.now(ZoneId.of("UTC")).toInstant))
      }

      val maybeDuplicatedVisit2 = generateVisit(2)
      val maybeDuplicatedVisit3 = generateVisit(3)
      val maybeDuplicatedVisit4 = generateVisit(4)
      val extraVisits = if (ThreadLocalRandom.current().nextBoolean()) {
        Seq(maybeDuplicatedVisit2, maybeDuplicatedVisit3, maybeDuplicatedVisit4)
      } else {
        Seq.empty
      }
      val visits = (Seq(
        generateVisit(1), generateVisit(1), generateVisit(1),
        generateVisit(2), generateVisit(2), maybeDuplicatedVisit2,
        generateVisit(3), generateVisit(3), maybeDuplicatedVisit3,
        generateVisit(4), maybeDuplicatedVisit4
      ) ++ extraVisits)

      visits.toDS().selectExpr("TO_JSON(STRUCT(*)) AS value")
        .write
        .format("kafka")
        .options(
          Map(
            "kafka.bootstrap.servers" -> "localhost:9094",
            "topic" -> InputDataTopicName,
          )
        )
        .save()

      Thread.sleep(TimeUnit.SECONDS.toMillis(5))
    }
  }

}
