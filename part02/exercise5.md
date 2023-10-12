# Exercise 5: an Apache Kafka and raw files sinks instead

Replace the ScyllaDB sink by Apache Kafka and JSON sinks. Both should write the same dataset.


<details>
<summary>Hints - Data sink</summary>

This time we need to change the data sink to `foreachBatch` and define the output definition inside as:

```
val datasetToWrite = dataset.cache()
datasetToWrite.write.mode(SaveMode.Overwrite).json(s"/tmp/wfc/workshop/part02/exercise4/${batchNumber}") 
datasetToWrite.selectExpr("decorated_value AS value").write.options(Map(
 "kafka.bootstrap.servers" -> "localhost:9094",
 "topic" -> EnrichedDataTopicName
)).format("kafka").save()
datasetToWrite.unpersist()
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
```
ls -R /tmp/wfc/workshop/part02/exercise4/
/tmp/wfc/workshop/part02/exercise4/:
17  18  19  20

/tmp/wfc/workshop/part02/exercise4/17:
part-00000-7a35ccdd-91bd-4841-b650-ce5c95b7ac8d-c000.json  part-00001-7a35ccdd-91bd-4841-b650-ce5c95b7ac8d-c000.json  _SUCCESS

/tmp/wfc/workshop/part02/exercise4/18:
part-00000-a442ef2b-7d63-4eda-a696-37cb5b1902ff-c000.json  part-00001-a442ef2b-7d63-4eda-a696-37cb5b1902ff-c000.json  _SUCCESS

/tmp/wfc/workshop/part02/exercise4/19:
part-00000-e4aba845-e60f-45cc-a646-d0baf2acdc34-c000.json  part-00001-e4aba845-e60f-45cc-a646-d0baf2acdc34-c000.json  _SUCCESS

/tmp/wfc/workshop/part02/exercise4/20:
part-00000-e7624673-5850-4f70-92d6-741005f6577a-c000.json  _SUCCESS

```

# Well done! 
⏭️ [start the next exercise](exercise6.md)
