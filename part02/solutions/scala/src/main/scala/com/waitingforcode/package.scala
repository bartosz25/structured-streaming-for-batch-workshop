package com

package object waitingforcode {

  val InputDataTopicName = "numbers"
  val EnrichedDataTopicName = "numbers_enriched"

  case class MappedEvent(value: String, decorated_value: String)
  case class MappedEventWithLabel(value: String, decorated_value: String, label: String)
}
