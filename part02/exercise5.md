# Exercise 5: an Apache Kafka and raw files sinks instead

Replace the ScyllaDB sink by Apache Kafka and JSON sinks. Both should write the same dataset.


<details>
<summary>Hints - Data sink</summary>

This time we need to change the data sink to `foreachBatch` and define the output definition inside as:

```
dataset.cache()
dataset.write.mode(SaveMode.Overwrite).json(s"/tmp/wfc/workshop/part02/exercise4/${batchNumber}")
// Kafka is not only available for the streaming API!
dataset.selectExpr("decorated_value AS value").write.options(Map(
  "kafka.bootstrap.servers" -> "localhost:9094",
  "topic" -> EnrichedDataTopicName
)).format("kafka").save()
```

Several points here:

* the `cache` is mandatory; otherwise the reading part is executed twice, once for each data sink
* defining 2 `writeStream...` on the data source won't work well because again, it'll involve reading the data twice
* moreover, 2 `writeStream`s bring the risk of reading different data since the data source is not synchronized; it
  breaks the requirement from the announcement
</details>
 
## Setup instructions
1. Implement the job.
2. Start the job.
3. Check the results in the *numbers_enriched* topic:
```
docker exec -ti docker_kafka_1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic numbers_enriched --from-beginning
```
4. Check the results in the output directory

# Well done! 
⏭️ [start the next exercises](/exercise6.md)
