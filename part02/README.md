# Exercise 1: simple mapping and Apache Kafka source

Your project stores some data in an Apache Kafka topic. Write an Apache Spark Structured Streaming pipeline to process this data continuously and prefix each event with the current timestamp. Write the output to the console sink.

Implement 2 pipelines, one with *SQL API* and another with *programmatic API (Python or Scala)*. 

The job should return 2 columns: initial one (`value`) and the one resulting from the concatenation operation.

Connection parameters for Apache Kafka data source:

* broker: localhost:9094
* topic: numbers

## Setup instructions

1. Start the Kafka broker and the ScyllaDB database we're going to use later:
```
cd part02/docker
docker-compose down --volumes; docker-compose up
```
2. Create the namespace for the ScyllaDB exercise:
```
docker exec docker_scylla_1 cqlsh -f /data_to_load.txt
```

3. Open the Scala or Python part in the IDE of your choice.
4. Implement the jobs.

<details>
<summary>Hints - data source definition</summary>

```
spark.readStream.format("kafka").option("..define your connection options here..")
```
</details>
	
<details>
<summary>Hints - data decoration</summary>

SQL:
```
CONCAT_WS(' ', 'Spark', 'SQL')
```

Python:
```
TODO:
```

Scala:
```
.map(..decoration logic here.)
```
</details>

<details>
<summary>Hints - data sink definition</summary>
```
.writeStream.format("console").option("truncate", false).option("checkpointLocation", "....")
```
</details>

5. Run the data generator: (`DataGenerator` in Scala, `data_generator.py` in Python).

6. Run the SQL job.

7. Stop the SQL job and start the Scala implementation.

# Exercise 2: Spark UI

Keep the previous pipeline running. Open the Spark UI at [http://localhost:4040](http://localhost:4040).

1. Where to find the information about the batch duration and data processing rate?
2. Stop the job and modify the trigger to `.trigger(Trigger.ProcessingTime("1 minute"))` (Scala) or `.trigger(processingTime='1 minute')` (Python)
3. Restart the job and open the Spark UI in the new tab or window. Compare the batch duration and data processing rate.
4. Do you see any difference?


<details>
<summary>Answers - Question 1</summary>
Under _Structured Streaming_ where the Streaming query statistics are displayed.
</details>

<details>
<summary>Answers - Question 4</summary>
Several aspects here:

* more rows are processed at once, the query waits 1 minute and takes more data at once
* it doesn't impact the execution time, though; our sink is not data-bounded; we print data which is stored in memory
  so there is no I/O impact because of the increased data volume; typically, it won't be the case if you use a I/O-bounded data sink
* there is less jobs executed in the Jobs section; it might be easier to follow
</details>

# Exercise 3: the streaming dataset with a Master Dataset enrichment

Enrich the job created before with a Master Dataset stored as JSON files under `/tmp/wfc/workshop/master/` directory. 

The keys responsible for the join are respectively:

* `value` for the Apache Kafka dataset
* `nr` for the Master Dataset

The enrichment process should keep the rows when they don't have matching values in the Master Dataset.

Besides, you can reduce the processing time trigger to `30 seconds` so that the join happens more often.

<details>
<summary>Hints - Master Dataset definition</summary>

It's a static dataset, so we use the Spark SQL API here:
```
sparkSession.read.schema("nr STRING, label STRING").json("/tmp/wfc/workshop/master")
```

Remember, for any kind of semi-structured data sources (JSON, CSV), it's important to explicitly define the schema. Otherwise Apache Spark will
sample the dataset to infer the schema which in the end can be wrong and the operation can be costly (dataset processed twice, for the schema resolution
and processing logic)
</details>

## Setup instructions
1. Run the following command to prepare the master dataset:
```
DATASET_DIR=/tmp/wfc/workshop/master/
mkdir -p $DATASET_DIR
echo '{"nr": "1", "label": "Number one"}
{"nr": "2", "label": "Number two"}' > "$DATASET_DIR/1_2.json"


echo '{"nr": "3", "label": "Number three"}
{"nr": "4", "label": "Number four"}' > "$DATASET_DIR/3_4.json"
```
2. Implement the job.
3. Start the data generator.
4. Start the job.
5. Add a new file to the Master Dataset:
```

echo '{"nr": "5", "label": "Number three"}
{"nr": "6", "label": "Number four"}' > "$DATASET_DIR/5_6.json"
```

6. Wait for the records number 5 and 6 to be joined and check the results of the job. You should see no matched rows because of the files index cache.
7. Rewrite the existing files of the job:
```
echo '{"nr": "1", "label": "ONE"}
{"nr": "2", "label": "TWO"}' > "$DATASET_DIR/1_2.json"

echo '{"nr": "3", "label": "THREE"}
{"nr": "4", "label": "FOUR"}' > "$DATASET_DIR/3_4.json"
```

7. Wait for the joins for 1, 2, 3, or 4 happening. You should see the rows joined with the updated JSONs.

*The refresh issue is present only for the raw data sources. Modern table file formats like Delta Lake don't have it.*


# Exercise 4: ScyllaDB sink

Change the sink from the console to ScyllaDB. We assume that we cannot use the ScyllaDB Spark connector here and must implement the sink differently.


<details>
<summary>Hints - Sink choice</summary>

There are multiple alternatives:

* `foreachPartition` with the ScyllaDB API
* `foreach` with a dedicated `ForeachWriter` that implements the open/process/close methods

My choice goes to the latter one since the API provides a quite nice pattern for the custom writing:
* in the `open` we're going to initialize the connection to the database
* in the `process` we're going to either add a record to the buffer or flush the buffer if it's full
* in the `close` we're going to flush remaining records from the buffer and close the ScyllaDB connection

Example for Scala:
```
class ScyllaDbWriter extends ForeachWriter[MappedEvent] {

  private var cqlSession: CqlSession = _
  
  private val bufferedRows = new mutable.ListBuffer[MappedEvent]()

  override def open(partitionId: Long, epochId: Long): Boolean = {
    cqlSession = CqlSession.builder()
      .withKeyspace("wfc")
      .build()
    true
  }

  override def process(visit: MappedEvent): Unit = {
    if (bufferedRows.size == 10) {
      flushBuffer()
    }
    bufferedRows.append(visit)
  }
  
  override def close(errorOrNull: Throwable): Unit = {
      flushBuffer()
      cqlSession.close()
  }	
```

Example for Python:
```
TODO:
```

</details>

## Setup instructions
1. Implement the job.
2. Start the job.
3. Check the results in the ScyllaDB output table:
```
docker exec -ti docker_scylla_1 cqlsh

Connected to  at 192.168.144.2:9042.
[cqlsh 5.0.1 | Cassandra 3.0.8 | CQL spec 3.3.1 | Native protocol v4]
Use HELP for help.
cqlsh> SELECT * FROM wfc.numbers;

 value | decorated_value
-------+----------------------------
     4 | 2023-09-21 05:41:04.828414
     5 | 2023-09-21 05:41:04.828415
     1 | 2023-09-21 05:41:04.828411

(3 rows)
```

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

# Exercise 6: fault-tolerance

Go to the checkpoint location and analyze the content:
```
cd /tmp/wfc/workshop/part02/checkpoint/

less commits/*
less offsets/*
```

Questions:

1. From your understanding, what is the logic behind the fault-tolerance? How the engine knows where to start processing if the job stops.
2. How to implement a reprocessing scenario from an arbitrary point in the past? What are your ideas?



<details>
<summary>Answers - Question 1</summary>
There is a versioned pair of `commits` and `offsets`. Whenever given micro-batch completes, it writes corresponding versioned files in these 2 directories. 
When you restart the job, Apache Spark verifies the last written version for both directories and:

* if both have the same number, the job starts in the next offset (N)
* if offset is higher than the commit, the job starts from the next to last offset (N-1)
* if commit is higher than offset, well, it can't happen unless you alter the checkpoint location on purpose
</details>

<details>
<summary>Answers - Question 2</summary>
Assuming the checkpoint data is still there - 10 most recent micro-batches are kept by default - you can:

* alter the last offset and rollback it to any point in the past
* explicitly set the starting timestamp in the job; but it requires changing the checkpoint location which in case of a stateful processing can be problematic
* remove the checkpoint metadata files you want to reprocess
 
**Before making any operation on the checkpoint location, create a backup**
</details>
