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
</details>


<details>
<summary>Hints - Scala ForeachWriter</summary>

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
</details>

<details>
<summary>Hints - Python ForeachWriter</summary>
Example for Python:

```
class ScyllaDbWriter:

    def __init__(self):
        self.session = None
        self.rows_to_send = None
        self.epoch_id = None

    def open(self, partition_id, epoch_id):
        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
            retry_policy=DowngradingConsistencyRetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=15,
            row_factory=tuple_factory
        )
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        self.session = cluster.connect('wfc')
        self.rows_to_send = []
        self.epoch_id = epoch_id
        return True

    def process(self, row):
        if len(self.rows_to_send) == 10:
            self._flush_buffer()
        self.rows_to_send.append(row)

    def close(self, error):
        self._flush_buffer()
        self.session.cluster.shutdown()
        print(f'Closing the writer for {self.epoch_id}')

    def _flush_buffer(self):
        insert_statements = []
        for row_to_insert in self.rows_to_send:
            insert_statements.append(f"INSERT INTO numbers (value, decorated_value, label) "
                                     f"VALUES ('{row_to_insert.value}', '{row_to_insert.decorated_value}', '{row_to_insert.label}');")

        insert_query = f"""
        BEGIN BATCH
            {"".join(insert_statements)}
        APPLY BATCH;
        """
        self.session.execute(insert_query)
        self.rows_to_send = []

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
⏭️ [start the next exercise](exercise5.md)
