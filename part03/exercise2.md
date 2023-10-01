# Exercise 2: write only the unique rows to the batch layer

Unfortunately, your data producers can deliver duplicated records from time to time. It adds an extra load to the consumers who need to perform 
compute-intensive deduplicates on big dataset.

Since you're responsible for the data ingestion part you were asked to implement a deduplication mechanism to the streaming pipeline. 

The mechanism doesn't have to be perfect and it may include some occasional duplicates, e.g. during a job restart. But it should help improving the 
data quality of the synchronized dataset.

The job to modify is:
* `Exercise2DistinctVisitsJob` for Scala
* `exercise2_distinct_visits_job.py` for Python

A duplicated event is the event with the same `event_id` and `event_time` fields.

## Setup instructions

1. Start the Kafka broker:
```
cd part03/docker
docker-compose down --volumes; docker-compose up
```

2. Open the Scala or Python directory in the IDE of your choice.
3. Implement the deduplication in the jobs. 

<details>
<summary>Hints - deduplication</summary>
You need to first declare the schema for the deduplication and later use it in the `dropDuplicates` method preceeded by the `withWatermark` operation. 
Without the watermark, the deduplication will keep the records forever which can lead to an OOM failure at some point due to too many records stored
in the state store.

PySpark:
```
event_schema_for_deduplication = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType())
])

query = input_data.selectExpr("CAST(value AS STRING)") \
    .select(functions.from_json("value", event_schema_for_deduplication).alias("value_struct"), "value") \
    .select("value_struct.event_time", "value_struct.event_id", "value") \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["event_id", "event_time"])\
    .drop("event_time", "event_id") # keep only the value which is required for the sink!
```

Scala Spark:
```
val eventSchema = StructType(Seq(
  StructField("event_id", StringType),
  StructField("event_time", TimestampType)
))


val query = inputDataStream.selectExpr("CAST(value AS STRING)")
  .select(functions.from_json($"value", eventSchema).alias("value_struct"), $"value")
  .select("value_struct.event_time", "value_struct.event_id", "value")
  .withWatermark("event_time", "10 minutes")
  .dropDuplicates(Seq("event_id", "event_time"))
  // keep only the value which is required for the sink !
  .drop("event_time", "event_id")
```
</details>

4. Run the data generator: (`DataGenerator` in Scala, `data_generator.py` in Python).

5. Run the job.

6. Stop the job and the data generator.

7. Test the output of the job in the output Kafka topic:
```
docker exec -ti docker_kafka_1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits_deduped --from-beginning
```

# 🥳 Congrats! You just completed some basic stateful processing tasks. You're ready now to go further 💪
