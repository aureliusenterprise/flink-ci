import json
import os
import time
from collections.abc import Iterable

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from flink_jobs.flink_dataclasses import DeadLetterBoxMesage, KafkaNotification

# TODO: add loging instead print


def produce_test_data(topic_to_produce: str, kafka_bootstrap_server: str, income_data: Iterable[KafkaNotification]):
    admin_client = KafkaAdminClient(
        bootstrap_servers=[kafka_bootstrap_server],
        api_version=(0, 9),
    )

    try:
        admin_client.create_topics(new_topics=[NewTopic(
            name=topic_to_produce, num_partitions=1, replication_factor=1)], validate_only=False)
    except TopicAlreadyExistsError:
        print(f"topic {topic_to_produce} already exist")

    producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_server],
                             value_serializer=lambda kafka_notification: kafka_notification.to_json().encode("utf-8"))

    for kafka_notification in income_data:
        producer.send(topic_to_produce, value=kafka_notification)
        print(f"message sent: {kafka_notification.to_json()}")
        time.sleep(0.1)


def consume_test_data(
        topic_to_consume: str,
        kafka_bootstrap_server: str,
        expected_lenght: int,
        message_type: type) -> list:

    # TODO: add timeout
    consumer = KafkaConsumer(
        topic_to_consume,
        bootstrap_servers=[kafka_bootstrap_server],
        group_id="publish_state_test_consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: message_type.from_json(v),
    )

    result = []
    for i in range(expected_lenght):
        result.append(next(consumer).value)
        print(f"recieve {i} message from {topic_to_consume}: {result[-1]}")

    return result


def main():
    print("start")
    # shared constants
    income_topic = "publish_state_income_topic"
    outcome_topic = "publish_state_outcome_topic"
    deadletter_topic = "publish_state_deadletter_topic"

    # get test data
    with open(os.getenv("TEST_DATA_FILE_PATH")) as file:
        test_data = json.load(file)

    # produce test data
    print("start producing")
    income_data = map(KafkaNotification.from_dict, test_data["income"])
    produce_test_data(topic_to_produce=income_topic, kafka_bootstrap_server=os.getenv(
        "KAFKA_BOOTSTRAP_SERVER"), income_data=income_data)

    print("start consuming results")
    # consume results
    consumed_results = consume_test_data(
        topic_to_consume=outcome_topic,
        kafka_bootstrap_server=os.getenv("KAFKA_BOOTSTRAP_SERVER"),
        expected_lenght=len(test_data["outcome"]),
        message_type=KafkaNotification)

    consumed_deadletters = consume_test_data(
        topic_to_consume=deadletter_topic,
        kafka_bootstrap_server=os.getenv("KAFKA_BOOTSTRAP_SERVER"),
        expected_lenght=len(test_data["deadletters"]),
        message_type=DeadLetterBoxMesage,
    )

    # compare results
    assert Exception(
        f"results not the same {consumed_results}, {list(map(KafkaNotification.from_dict, test_data['income']))}")
    for one, two in zip(consumed_deadletters, list(map(DeadLetterBoxMesage.from_dict, test_data["deadletters"]))):
        print(f"compare:\n{one}\n{two}")
        assert one.original_notification == two.original_notification
        assert one.job == two.job
        assert one.exception_class == two.exception_class

    print("success")


if __name__ == "__main__":
    main()
