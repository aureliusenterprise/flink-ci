import json
import logging
import os
import sys
from pathlib import Path
from typing import TypedDict

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer


class SynchronizeAppSearchConfig(TypedDict):
    """
    Configuration required to execute the SynchronizeAppSearch job.

    Attributes
    ----------
    kafka_app_search_topic_name : str
        The name of the Kafka topic to sink the messages to.
    kafka_bootstrap_server_hostname : str
        The hostname of the Kafka bootstrap server.
    kafka_bootstrap_server_port : str
        The port number of the Kafka bootstrap server.
    kafka_producer_group_id : str
        The Kafka producer group ID to use.
    """

    kafka_app_search_topic_name: str
    kafka_bootstrap_server_hostname: str
    kafka_bootstrap_server_port: str
    kafka_producer_group_id: str


def main(config: SynchronizeAppSearchConfig) -> None:
    """Sink an example message into a Kafka topic."""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add JARs to the classpath
    jars = [path.absolute().as_uri() for path in Path("./jars").glob("*.jar")]
    env.add_jars(*jars)

    kafka_bootstrap_server = (
        f"{config['kafka_bootstrap_server_hostname']}:{config['kafka_bootstrap_server_port']}"
    )

    # Set up the Kafka sink
    kafka_sink = FlinkKafkaProducer(
        topic=config["kafka_app_search_topic_name"],
        producer_config={
            "bootstrap.servers": kafka_bootstrap_server,
            "max.request.size": "14999999",
            "group.id": config["kafka_producer_group_id"],
        },
        serialization_schema=SimpleStringSchema(),
    )

    documents = [
        {
            "title": "Example Message",
            "description": "This is an example message.",
            "url": "https://example.com/example-message",
        },
    ]

    stream = env.from_collection(documents, Types.MAP(Types.STRING(), Types.STRING()))

    pipeline = stream.map(
        json.dumps,
        Types.STRING(),
    )

    pipeline.add_sink(kafka_sink).name("Kafka Sink")

    env.execute("Synchronize App Search")


if __name__ == "__main__":
    """
    Entry point of the script. Load configuration from environment variables and start the job.
    """
    config: SynchronizeAppSearchConfig = {
        "kafka_app_search_topic_name": "app_search_documents",
        "kafka_bootstrap_server_hostname": os.environ["KAFKA_BOOTSTRAP_SERVER_HOSTNAME"],
        "kafka_bootstrap_server_port": os.environ["KAFKA_BOOTSTRAP_SERVER_PORT"],
        "kafka_producer_group_id": os.environ["KAFKA_PRODUCER_GROUP_ID"],
    }

    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    main(config)
