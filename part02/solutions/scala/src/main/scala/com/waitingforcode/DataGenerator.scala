package com.waitingforcode

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.sys.process.Process
import scala.util.Random

object DataGenerator {

  def main(args: Array[String]): Unit = {
    Seq(InputDataTopicName, EnrichedDataTopicName).foreach(topicToRecreate => {
      println(s"== Deleting already existing topic ${topicToRecreate} ==")
      val deleteTopicResult =
        Process(s"docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic ${topicToRecreate} --delete").run()
      deleteTopicResult.exitValue()
      println(s"== Creating ${topicToRecreate} ==")
      val createTopicResult =
        Process(s"docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic ${topicToRecreate} --create --partitions 2").run()
      createTopicResult.exitValue()
    })


    val kafkaProducer = new KafkaProducer[String, String](Map[String, Object](
      "bootstrap.servers" -> "localhost:9094",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    ).asJava)

    var number = 1
    while (true) {
      println(">>> Generating records")
      kafkaProducer.send(new ProducerRecord[String, String](InputDataTopicName, s"${number}"))
      kafkaProducer.send(new ProducerRecord[String, String](InputDataTopicName, s"${number}"))
      kafkaProducer.send(new ProducerRecord[String, String](InputDataTopicName, s"${number}"))
      kafkaProducer.send(new ProducerRecord[String, String](InputDataTopicName, s"${number}"))
      kafkaProducer.send(new ProducerRecord[String, String](InputDataTopicName, s"${number}"))
      kafkaProducer.send(new ProducerRecord[String, String](InputDataTopicName, s"${number}"))
      kafkaProducer.flush()

      number = new Random().nextInt(6)
      Thread.sleep(TimeUnit.SECONDS.toMillis(5))
    }

  }

}
