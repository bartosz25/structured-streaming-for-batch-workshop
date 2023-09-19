# Exercise 1: simple mapping and Apache Kafka source

Your project stores some data in an Apache Kafka topic. Write an Apache Spark Structured Streaming pipeline to process this data continuously and prefix each event with the current timestamp. Write the output to the console sink.

Implement 2 pipelines, one with SQL API and another with programmatic API (Python or Scala). 

## Setup instructions

<details>
	<summary>Hints</summary>
	### Data source definition
	```
	spark.readStream.format("kafka").option("", "")
	```
	
	### Data decoration - SQL
	
	### Data decoration - Python
	
	### Data decoration - Scala
	
	### Console sink
</details>
