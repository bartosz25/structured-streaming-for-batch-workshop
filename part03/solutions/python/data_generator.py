import json
import os
import random
import uuid
from datetime import datetime
import time
from typing import Dict

from pyspark.sql import SparkSession

from config import kafka_input_topic, kafka_deduped_topic

for topic in [kafka_input_topic(), kafka_deduped_topic()]:
    print('>>> Recreating topics...')
    os.system(
        f'docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic {topic} --delete')
    os.system(
        f'docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic {topic} --create --partitions 2')

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()


def visit(visit_id: int) -> Dict[str, str]:
    return {'value': json.dumps({'event_id': str(uuid.uuid4()), 'visit_id': visit_id,
                                 'event_time': datetime.now().isoformat(),
                                 'page': random.choice(
                                     ["page1", "page2", "page3", "page4", "page5"])})}


while True:
    print('Generating data')
    maybe_duplicated_visit_2 = visit(2)
    maybe_duplicated_visit_3 = visit(3)
    maybe_duplicated_visit_4 = visit(4)
    duplicates = []

    if bool(random.getrandbits(1)):
        duplicates.append(maybe_duplicated_visit_2)
        duplicates.append(maybe_duplicated_visit_3)
        duplicates.append(maybe_duplicated_visit_4)

    dataset = [visit(1), visit(1), visit(1),
               visit(2), visit(2), maybe_duplicated_visit_2,
               visit(3), visit(3), maybe_duplicated_visit_3,
               visit(4), maybe_duplicated_visit_4] + duplicates

    dataframe_to_write = spark.createDataFrame(dataset, "value STRING") \
        .selectExpr("value")
    dataframe_to_write.write.format('kafka').option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('topic', kafka_input_topic()).save()

    time.sleep(5)