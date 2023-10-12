package com.waitingforcode

import org.apache.spark.sql.SparkSession

object NumbersProcessing {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/wfc/workshop/part02/checkpoint"
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  }

}