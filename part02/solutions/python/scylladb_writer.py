from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory


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
