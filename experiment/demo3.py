
import json
import logging
import os
import uuid
from pathlib import Path
from typing import TypedDict

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.elasticsearch import (
    Elasticsearch7SinkBuilder,
    ElasticsearchEmitter,
    FlushBackoffType,
)
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer


class PublishStateConfig(TypedDict):
    """
    Configuration required to execute the PublishState job.

    Attributes
    ----------
    elasticsearch_endpoint: str
        The endpoint URL for the Elasticsearch instance.
    elasticsearch_username: str
        The username for Elasticsearch authentication.
    elasticsearch_password: str
        The password for Elasticsearch authentication.
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
    """

    elasticsearch_endpoint: str
    elasticsearch_username: str
    elasticsearch_password: str
    kafka_bootstrap_server_hostname: str
    kafka_bootstrap_server_port: str
    kafka_consumer_group_id: str
    kafka_error_topic_name: str
    kafka_producer_group_id: str
    kafka_source_topic_name: str

def log(data:dict) -> dict:
    """
    log.

    Logging the message and turn it into a generic format.
    """
    logging.error(data)
    return {"id": data["id"], "msg": json.dumps(data)}

def mapping(data:str) -> dict:
    """
    map.

    transform the incoming message.
    """
    data2=data
    id_= str(uuid.uuid4())
    return json.loads(json.dumps({"id":id_, "msg": data2}))

def mapping2(data:str) -> dict:
    """
    map.

    transform the incoming message.
    """
    return json.loads(json.dumps(data))

def mapping3(data:str) -> dict:
    """
    map.

    transform the incoming message.
    """
    return json.loads(data)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    config: PublishStateConfig = {
        "elasticsearch_endpoint": os.environ["ELASTICSEARCH_ENDPOINT"],
        "elasticsearch_username": os.environ["ELASTICSEARCH_USERNAME"],
        "elasticsearch_password": os.environ["ELASTICSEARCH_PASSWORD"],
        "kafka_bootstrap_server_hostname": os.environ["KAFKA_BOOTSTRAP_SERVER_HOSTNAME"],
        "kafka_bootstrap_server_port": os.environ["KAFKA_BOOTSTRAP_SERVER_PORT"],
        "kafka_consumer_group_id": os.environ["KAFKA_CONSUMER_GROUP_ID"],
        "kafka_error_topic_name": os.environ["KAFKA_ERROR_TOPIC_NAME"],
        "kafka_producer_group_id": os.environ["KAFKA_PRODUCER_GROUP_ID"],
        "kafka_source_topic_name": os.environ["KAFKA_SOURCE_TOPIC_NAME"],
    }

    env = StreamExecutionEnvironment.get_execution_environment()

    # write all the data to one file
    env.set_parallelism(1)

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

    # Set up the Elasticsearch sink
    es_sink = Elasticsearch7SinkBuilder() \
        .set_emitter(ElasticsearchEmitter.static_index("foo", "id")) \
        .set_hosts(["localhost:9200"]) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_bulk_flush_max_actions(1) \
        .set_bulk_flush_max_size_mb(2) \
        .set_bulk_flush_interval(1000) \
        .set_bulk_flush_backoff_strategy(FlushBackoffType.CONSTANT, 3, 3000) \
        .set_connection_username("elastic") \
        .set_connection_password("elasticpw") \
        .set_connection_request_timeout(30000) \
        .set_connection_timeout(31000) \
        .set_socket_timeout(32000) \
        .build()

    ds =( input_stream
        .map(mapping3, Types.MAP(Types.STRING(),Types.STRING()))
        .map(log, Types.MAP(Types.STRING(), Types.STRING()))
    )
    ds.sink_to(es_sink)

    env.execute()

# example message  {"name": "ada","id": "5"}
