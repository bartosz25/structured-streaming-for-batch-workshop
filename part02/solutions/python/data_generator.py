import os
import random
import time

from confluent_kafka import Producer

from config import kafka_input_topic, kafka_input_enriched_topic

for topic_to_create in [kafka_input_topic(), kafka_input_enriched_topic()]:
    print(f'Deleting the {topic_to_create}')
    os.system(f'docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic {topic_to_create} --delete')
    print(f'Creating the {topic_to_create}')
    os.system(f'docker exec docker_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --topic {topic_to_create} --create --partitions 2')

producer = Producer({'bootstrap.servers': 'localhost:9094'})

number = 0
while True:
    print('>>>> Generating records')
    producer.produce(topic=kafka_input_topic(), value=f'{number}'.encode('UTF-8'))
    producer.produce(topic=kafka_input_topic(), value=f'{number}'.encode('UTF-8'))
    producer.produce(topic=kafka_input_topic(), value=f'{number}'.encode('UTF-8'))
    producer.produce(topic=kafka_input_topic(), value=f'{number}'.encode('UTF-8'))
    producer.produce(topic=kafka_input_topic(), value=f'{number}'.encode('UTF-8'))
    producer.produce(topic=kafka_input_topic(), value=f'{number}'.encode('UTF-8'))
    producer.flush()

    number = random.randint(0, 6)
    time.sleep(5)
