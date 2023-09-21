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

# Well done! 
⏭️ [start the next exercises](exercise2.md)
