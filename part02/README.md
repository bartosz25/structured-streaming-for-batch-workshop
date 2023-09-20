# Exercise 1: simple mapping and Apache Kafka source

Your project stores some data in an Apache Kafka topic. Write an Apache Spark Structured Streaming pipeline to process this data continuously and prefix each event with the current timestamp. Write the output to the console sink.

Implement 2 pipelines, one with *SQL API* and another with *programmatic API (Python or Scala)*. 

The job should return 2 columns: initial one (`value`) and the one resulting from the concatenation operation.

Connection parameters for Apache Kafka data source:

* broker: localhost:9094
* topic: numbers

## Setup instructions

1. Start the Kafka broker:
```
cd part02/docker
docker-compose down --volumes; docker-compose up
```
2. Open the Scala or Python part in the IDE of your choice.
3. Implement the jobs.

<details>
	<summary>Hints</summary>

	### Data source definition
	```
	spark.readStream.format("kafka").option("..define your connection options here..")
	```
	
	### Data decoration - SQL
	```
	CONCAT_WS(' ', 'Spark', 'SQL')
	```
	
	### Data decoration - Python
	```
	TODO:
	```
	
	### Data decoration - Scala
	```
	.map(..decoration logic here.)
	```
	
	### Console sink
	```
	.writeStream.format("console").option("truncate", false).option("checkpointLocation", "....")
	```
</details>

4. Run the data generator: (`DataGenerator` in Scala, `data_generator.py` in Python).

5. Run the SQL job.

6. Stop the SQL job and run the Scala implementation.

# Exercise 2: Spark UI

Keep the previous pipeline running. Open the Spark UI at [http://localhost:4040](http://localhost:4040).

1. Where to find the information about the batch duration and data processing rate?
2. Stop the job and modify the trigger to `.trigger(Trigger.ProcessingTime("1 minute"))` (Scala) or `.trigger(processingTime='1 minute')` (Python)
3. Restart the job and open the Spark UI in the new tab or window. Compare the batch duration and data processing rate.
4. Do you see any difference?


<details>
	<summary>Answers</summary>

	### Question 1.
	Under _Structured Streaming_ where the Streaming query statistics are displayed.
	
	### Question 4.
	Several aspects here:
	
	* more rows are processed at once, the query waits 1 minute and takes more data at once
	* it doesn't impact the execution time, though; our sink is not data-bounded; we print data which is stored in memory
	  so there is no I/O impact because of the increased data volume; typically, it won't be the case if you use a I/O-bounded data sink
	* there is less jobs executed in the Jobs section; it might be easier to follow
	
	```
</details>

# Exercise 3: the streaming dataset with a Master Dataset enrichment

Enrich the job created before with a Master Dataset stored as JSON files under `/tmp/wfc/workshop/master/` directory. 

The keys responsible for the join are respectively:

* `value` for the Apache Kafka dataset
* `nr` for the Master Dataset

The enrichment process should keep the rows without the matches in the Master Dataset.

Besides, you can reduce the processing time trigger to `30 seconds`.

## Setup instructions
1. Run the following command:
```
DATASET_DIR=/tmp/wfc/workshop/master/
mkdir -p $DATASET_DIR
echo '{"nr": "1", "label": "Number one"}
{"nr": "2", "label": "Number two"}' > "$DATASET_DIR/1_2.json"


echo '{"nr": "3", "label": "Number three"}
{"nr": "4", "label": "Number four"}' > "$DATASET_DIR/3_4.json"
```
2. Start the job.
3. Add a new file to the Master Dataset:
```

echo '{"nr": "5", "label": "Number three"}
{"nr": "6", "label": "Number four"}' > "$DATASET_DIR/5_6.json"
```

4. Check the results of the job.
5. Rewrite the existing files of the job:
```
echo '{"nr": "1", "label": "ONE"}
{"nr": "2", "label": "TWO"}' > "$DATASET_DIR/1_2.json"

echo '{"nr": "3", "label": "THREE"}
{"nr": "4", "label": "FOUR"}' > "$DATASET_DIR/3_4.json"
```

*The refresh issue is present only for the raw data sources. Modern table file formats like Delta Lake don't have it.* TODO: explain why

<details>
	<summary>Hints</summary>

	### Master Dataset definition
	
	It's a static dataset, so we use the Spark SQL API here:
	```
	sparkSession.read.schema("nr STRING, label STRING").json("/tmp/wfc/workshop/master")
	```
	
	Remember, for any kind of semi-structured data sources (JSON, CSV), it's important to explicitly define the schema. Otherwise Apache Spark will
	sample the dataset to infer the schema which in the end can be wrong and the operation can be costly (dataset processed twice, for the schema resolution
	and processing logic)
	```
</details>

# Exercise 4: ScyllaDB sink

Change the sink from the console to ScyllaDB. We assume that we cannot use the ScyllaDB Spark connector here and must implement the sink differently.


<details>
	<summary>Hints</summary>

	### Sink choice
	
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
	
	```
</details>

# Exercise 5: an Apache Kafka and raw files sinks instead

Replace the ScyllaDB sink by Apache Kafka and JSON sinks. Both should write the same dataset.


<details>
	<summary>Hints</summary>

	### Data sink
	
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
 
# Exercise 6: fault-tolerance

Go to the checkpoint location and analyze the content:
```
TODO:commands
```

Questions:

1. From your understanding, what is the logic behind the fault-tolerance? How the engine knows where to start processing if the job stops.
2. How to implement a reprocessing scenario from an arbitrary point in the past? What are your ideas?
