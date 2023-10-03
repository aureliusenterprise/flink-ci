from time import sleep
import json
import logging
import os
import datetime
import sys

from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')

admin_client = KafkaAdminClient(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    api_version = (0, 9),
)

TOPIC_TO_PRODUCE = "test-topic"


# create topics
try:
    admin_client.create_topics(new_topics=[NewTopic(name=TOPIC_TO_PRODUCE, num_partitions=1, replication_factor=1)], validate_only=False)
    logging.info(f"topic {TOPIC_TO_PRODUCE} created")
except TopicAlreadyExistsError:
    logging.info(f"topic {TOPIC_TO_PRODUCE} already exist")

# produce data
producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

data = {"id": 1, "data": u"Name", "create_ts": datetime.datetime.now().strftime("%H:%M:%S.%f")}
producer.send(TOPIC_TO_PRODUCE, value=data)

logging.info(f"message send: {data}")

# consume data
consumer = KafkaConsumer(
    TOPIC_TO_PRODUCE,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    group_id="test-consumer-group",
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

for message in consumer:
    assert message.value == data
    break