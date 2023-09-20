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

Keep the previous pipeline running. Open the Spark UI at (http://localhost:4040)[http://localhost:4040].

1. Where to find the information about the batch duration and data processing rate?
2. Stop the job and modify the trigger to `.trigger(Trigger.ProcessingTime("1 minute"))` TODO: find for Python.
3. Restart the job and open the Spark UI in the new tab or window. Compare the batch duration and data processing rate.
4. Do you see any difference?


<details>
	<summary>Answers</summary>

	### Question 1.
	
	
	### Question 4.
	
	
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


# Exercise 4: ScyllaDB sink

Change the sink from the console to ScyllaDB. We assume that we cannot use the ScyllaDB Spark connector here and must implement the sink differently.

# Exercise 5: an Apache Kafka and raw files sinks instead

Replace the ScyllaDB sink by Apache Kafka and JSON sinks. Both should write the same dataset.

 

