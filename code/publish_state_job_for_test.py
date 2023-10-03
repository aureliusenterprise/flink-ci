import logging
import os

from flink_jobs_tests.test_plug_elastic_client import TestPlugElasticClient

from flink_jobs.jobs_runners.publish_state_job import run_publish_state_job


def main():
    logging.info("start")
    # shared constants
    income_topic = "publish_state_income_topic"
    outcome_topic = "publish_state_outcome_topic"
    deadletter_topic = "publish_state_deadletter_topic"
    consumer_group = "publish_state_consumer"
    producer_group = "publish_state_producer"

    # setup test elastic client data
    elastic_client = TestPlugElasticClient()

    # Run flink job
    # TODO: ConfigStore
    logging.info("start flink job")
    {
        "kafka.bootstrap.server.hostname": os.getenv("KAFKA_BOOTSTRAP_SERVER_HOSTNAME"),
        "kafka.bootstrap.server.port": os.getenv("KAFKA_BOOTSTRAP_SERVER_PORT"),
        "enriched.events.topic.name": income_topic,
        "sink.events.topic.name": outcome_topic,
        "deadlettterbox.topic.name": deadletter_topic,
        "kafka.consumer.publish_state.group.id": consumer_group,
        "kafka.producer.publish_state.group.id": producer_group,
        "elastic.search.endpoint": "elasticsearch:9200",
        "elastic.cloud.username": "user",
        "elastic.cloud.password": "password",
    }
    run_publish_state_job(elastic_client)

    logging.info("SUCCESS")

if __name__ == "__main__":
    main()
