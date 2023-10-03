# Exercise 1: count the number of items in a tumbling window

Your streaming project is mature enough to start creating the first business use cases. 

The first business demand is to count the number of visits in 15-seconds non-overlapping windows.

Connection parameters for the Apache Kafka data source:

* broker: localhost:9094
* topic: visits

Write the results to the console sink.

## Setup instructions

1. Start the Kafka broker:
```
cd part03/docker
docker-compose down --volumes; docker-compose up
```

2. Open the Scala or Python directory in the IDE of your choice.
3. Implement the jobs. If you want, you can start with the available templates, `Exercise1WindowJob` for Scala or `exercise1_window_job.py` for Python

<details>
<summary>Hints - window definition</summary>

In PySpark a tumbling window is defined by the window key and the window duration in the group by operator:
```
query = input_data.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json("value", event_schema).alias("value_struct"), "value") \
    .select("value_struct.event_time", "value_struct.event_id", "value") \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(F.window(F.col("event_time"), "15 seconds")).count()
```

To see the watermark in action you can add a column with the current timestamp after the `..count()` like
`withColumn("current_time", F.current_timestamp())`.

The same is valid for Scala Spark:
```
val windowCount = inputDataStream.selectExpr("CAST(value AS STRING)")
 .select(functions.from_json($"value", eventSchema).alias("value_struct"), $"value")
 .select("value_struct.event_time", "value_struct.event_id", "value")
 .withWatermark("event_time", "10 seconds")
 .groupBy(functions.window($"event_time", "15 seconds"))
 .count()
 .withColumn("current_time", functions.current_timestamp())
```
</details>

4. Run the data generator: (`DataGenerator` in Scala, `data_generator.py` in Python).

5. Run the window job.

6. Stop the job and the generator.

# Well done! 
⏭️ [start the next exercise](exercise2.md)

