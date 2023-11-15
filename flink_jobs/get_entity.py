import logging
import os
import sys
from pathlib import Path
from typing import TypedDict

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer

from flink_tasks import GetEntity


class GetEntityConfig(TypedDict):
    """
    Configuration required to execute the PublishState job.

    Attributes
    ----------
    atlas_endpoint: str
        The endpoint URL for the Atlas instance.
    atlas_username: str
        The username for Atlas authentication.
    atlas_password: str
        The password for Atlas authentication.
    kafka_bootstrap_server_hostname: str
        The hostname of the Kafka bootstrap server.
    kafka_bootstrap_server_port: str
        The port number of the Kafka bootstrap server.
    kafka_consumer_group_id: str
        The consumer group ID for Kafka.
    kafka_error_topic_name: str
        The Kafka topic name where errors will be published.
    kafka_producer_group_id: str
        The producer group ID for Kafka.
    kafka_source_topic_name: str
        The Kafka topic name from which data will be consumed.
    kafka_target_topic_name: str
        The Kafka topic name to which data will be published.
    """

    atlas_endpoint: str
    atlas_username: str
    atlas_password: str
    kafka_bootstrap_server_hostname: str
    kafka_bootstrap_server_port: str
    kafka_consumer_group_id: str
    kafka_error_topic_name: str
    kafka_producer_group_id: str
    kafka_source_topic_name: str
    kafka_target_topic_name: str

def main(config: GetEntityConfig) -> None:
    """
    Execute the `GetEntity` Flink job.

    This function sets up the data stream from Kafka, processes it using the `GetEntity` logic,
    and then sinks the data to a new Kafka topic and errors to another Kafka topic.

    Parameters
    ----------
    config : GetEntityConfig
        The configuration required to execute the job.
    """
    env = StreamExecutionEnvironment.get_execution_environment()

     # Add JARs to the classpath
    jars = [path.absolute().as_uri() for path in Path("./jars").glob("*.jar")]
    env.add_jars(*jars)

    kafka_host = config["kafka_bootstrap_server_hostname"]
    kafka_port = config["kafka_bootstrap_server_port"]
    kafka_bootstrap_server = f"{kafka_host}:{kafka_port}"

    # Set up the input stream based on a Kafka consumer
    kafka_consumer = (
        FlinkKafkaConsumer(
            topics=config["kafka_source_topic_name"],
            properties={
                "bootstrap.servers": kafka_bootstrap_server,
                "group.id": config["kafka_consumer_group_id"],
            },
            deserialization_schema=SimpleStringSchema(),
        )
        .set_commit_offsets_on_checkpoints(commit_on_checkpoints=True)
        .set_start_from_latest()
    )

    input_stream = env.add_source(kafka_consumer).name("Kafka Source")

    # Set up the error sink
    error_sink = FlinkKafkaProducer(
        topic=config["kafka_error_topic_name"],
        producer_config={
            "bootstrap.servers": kafka_bootstrap_server,
            "max.request.size": "14999999",
            "group.id": config["kafka_producer_group_id"],
        },
        serialization_schema=SimpleStringSchema(),
    )

    kafka_sink = FlinkKafkaProducer(
        topic=config["kafka_target_topic_name"],
        producer_config={
            "bootstrap.servers": kafka_bootstrap_server,
            "max.request.size": "14999999",
            "group.id": config["kafka_producer_group_id"],
        },
        serialization_schema=SimpleStringSchema(),
    )


    # Set up the Elasticsearch sink

    get_entity = GetEntity(input_stream,
                           credentials=(config["atlas_username"],
                                        config["atlas_password"]))
    get_entity.input_validation.main.map(str, Types.STRING()).\
                    add_sink(kafka_sink).name("kafka Sink")
    get_entity.errors.map(str, Types.STRING()).add_sink(error_sink).name("Error Sink")

    env.execute("Get Entity")


if __name__ == "__main__":
    """
    Entry point of the script. Load configuration from environment variables and start the job.
    """
    config: GetEntityConfig = {
        "atlas_endpoint": os.environ["ATLAS_ENDPOINT"],
        "atlas_username": os.environ["ATLAS_USERNAME"],
        "atlas_password": os.environ["ATLAS_PASSWORD"],
        "kafka_bootstrap_server_hostname": os.environ["KAFKA_BOOTSTRAP_SERVER_HOSTNAME"],
        "kafka_bootstrap_server_port": os.environ["KAFKA_BOOTSTRAP_SERVER_PORT"],
        "kafka_consumer_group_id": os.environ["KAFKA_CONSUMER_GROUP_ID"],
        "kafka_error_topic_name": os.environ["KAFKA_ERROR_TOPIC_NAME"],
        "kafka_producer_group_id": os.environ["KAFKA_PRODUCER_GROUP_ID"],
        "kafka_source_topic_name": os.environ["KAFKA_SOURCE_TOPIC_NAME"],
        "kafka_target_topic_name": os.environ["KAFKA_ATLAS_AUDIT_TOPIC_NAME"],
    }

    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")

    main(config)
