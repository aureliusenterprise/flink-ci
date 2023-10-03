
from pyflink.datastream import DataStream

from flink_jobs.elastic_client import ElasticClient

from .get_previous_entity import GetPreviousEntity
from .prepare_notification_to_index import PrepareNotificationToIndex
from .verify_kafka_notification import ValidateKafkaNotifications


class PublishState:
    """
    Represents the Publish State job in Apache Flink.

    Attributes
    ----------
    data_stream : DataStream
        The data stream to process.
    elastic_client : ElasticClient
        The client to interact with Elasticsearch.
    """

    def __init__(
        self,
        data_stream: DataStream,
        elastic_client: ElasticClient,
    ) -> None:
        """
        Initialize the PublishStateJob object.

        Parameters
        ----------
        data_stream : DataStream
            The data stream to process.
        elastic_client : ElasticClient
            The client to interact with Elasticsearch.
        """
        self.data_stream = data_stream
        self.elastic_client = elastic_client

        self.input_validation = ValidateKafkaNotifications(self.data_stream)

        self.previous_entity_retrieval = GetPreviousEntity(
            self.input_validation.validated_notifications,
            self.elastic_client,
        )

        self.index_preparation = PrepareNotificationToIndex(
            self.input_validation.validated_notifications,
        )

        self.dead_letter = self.input_validation.schema_validation_errors.union(
            self.input_validation.no_entity_errors,
            self.previous_entity_retrieval.lookup_errors,
        )
