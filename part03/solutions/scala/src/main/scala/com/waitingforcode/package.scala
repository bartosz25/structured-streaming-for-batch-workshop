package com

import java.sql.Timestamp
import scala.util.Random

package object waitingforcode {

  val InputDataTopicName = "visits"
  val DedupedDataTopicName = "visits_deduped"

  case class Visit(event_id: String, visit_id: Int, event_time: Timestamp,
                   page: String = Random.shuffle(List("page1", "page2", "page3", "page4", "page5")).head)
}
