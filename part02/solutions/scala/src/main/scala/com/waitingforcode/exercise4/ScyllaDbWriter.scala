package com.waitingforcode.exercise4

import com.datastax.oss.driver.api.core.CqlSession
import com.waitingforcode.MappedEventWithLabel
import org.apache.spark.sql.ForeachWriter

import scala.collection.mutable

class ScyllaDbWriter extends ForeachWriter[MappedEventWithLabel] {

  private var cqlSession: CqlSession = _

  private val bufferedRows = new mutable.ListBuffer[MappedEventWithLabel]()

  override def open(partitionId: Long, epochId: Long): Boolean = {
    cqlSession = CqlSession.builder()
      .withKeyspace("wfc")
      .build()
    true
  }

  override def process(visit: MappedEventWithLabel): Unit = {
    if (bufferedRows.size == 10) {
      flushBuffer()
    }
    bufferedRows.append(visit)
  }

  override def close(errorOrNull: Throwable): Unit = {
    flushBuffer()
    cqlSession.close()
  }

  private def flushBuffer(): Unit = {
    val insertStatements = bufferedRows.map(row =>
      s"""
         |INSERT INTO numbers (value, decorated_value, label)
         |VALUES ('${row.value}', '${row.decorated_value}', '${row.label}');
         |""".stripMargin).mkString("\n")

    val insertBatchCommand =
      s"""
         |BEGIN BATCH
         |${insertStatements}
         |APPLY BATCH;
         |""".stripMargin

    cqlSession.execute(insertBatchCommand)
    bufferedRows.clear()
  }
}
