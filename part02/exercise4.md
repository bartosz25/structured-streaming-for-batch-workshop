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


# Well done! 
⏭️ [start the next exercises](exercise5.md)
