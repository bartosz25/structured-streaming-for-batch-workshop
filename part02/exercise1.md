# Exercise 1: simple mapping and Apache Kafka source

Your project stores some data in an Apache Kafka topic. Write an Apache Spark Structured Streaming pipeline to process this data continuously and prefix each event with the current timestamp. Write the output to the console sink.

Implement 2 pipelines, one with *SQL API* and another with *programmatic API (Python or Scala)*. 

The job should return 2 columns: initial one (`value`) and the one resulting from the concatenation operation.

Connection parameters for the Apache Kafka data source:

* broker: localhost:9094
* topic: numbers

## Setup instructions

1. Start the Kafka broker and the ScyllaDB database we're going to use later:
```
cd part02/docker
docker-compose down --volumes; docker-compose up
```

2. Open the Scala or Python directory in the IDE of your choice.
3. Implement the jobs. If you want, you can start with the available templates, `NumbersProcessing` for Scala or `numbers_processing.py` for Python

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
def concat_values_with_now(rows):
    for row in rows:
        row["decorated_value"] = row["current_timestamp"].astype(str) + " >>> " + row["value"]
        yield row


mapped_numbers = numbers.mapInPandas(concat_values_with_now, "value STRING, decorated_value STRING, current_timestamp TIMESTAMP")
```
You just discovered the hard way that there are some differences between PySpark and Scala API for Structured Streaming 💪
PySpark relies a lot on the `*inPandas` operations which work on a batch of records instead of on one record at once. 
The reason for that is due to the serialization/deserialization loops that are being constly when Python VM exchanges the data with the JVM.
Those `inPandas` operations reduce the overhead drastically. 

Scala:
```
.map(..decoration logic here.)
```
</details>

<details>
<summary>Hints - data sink definition</summary>

```
.writeStream.format("console").option("truncate", false)
```
</details>

<details>
<summary>Hints - checkpoint location</summary>

```
.writeStream.format("console").option("truncate", false).option("checkpointLocation", "....")
```

The checkpoint location is not mandatory for the exercises but we're going to use it in the last exercise, so to simplify the code evolution, 
it's more convenient to set it right now.
</details>

<details>
<summary>Hints - running the streaming query</summary>

```
val writeQuery = ....writeStream.format("console").option("truncate", false).option("checkpointLocation", "....")

writeQuery.start().awaitTermination()
```

If you don't call `start()`, your streaming query won't start. If you do but forget the `awaitTermination()`, the query will start and stop soon after.
</details>

5. Run the data generator: (`DataGenerator` in Scala, `data_generator.py` in Python).

6. Run the SQL job.

7. Stop the SQL job and start the Scala implementation.

# Well done! 
⏭️ [start the next exercise](exercise2.md)
