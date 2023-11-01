from pyflink.datastream import DataStream

from tasks.elastic_client import ElasticClient

from .operations import GetPreviousEntity, PrepareNotificationToIndex, ValidateKafkaNotifications


class PublishState:
    """
    A class that orchestrates the entire process of handling Kafka notifications.

    This class initializes the various processing stages, including:

    - Validation of input Kafka notifications
    - Retrieving previous entity versions from Elasticsearch
    - Preparing the validated notifications for indexing.

    Attributes
    ----------
    data_stream : DataStream
        The input Kafka notifications stream.
    input_validation : ValidateKafkaNotifications
        Stage for validating input Kafka notifications.
    previous_entity_retrieval : GetPreviousEntity
        Stage for retrieving the previous entity versions from Elasticsearch.
    index_preparation : PrepareNotificationToIndex
        Stage for preparing the validated notifications for indexing.
    errors : DataStream
        Aggregated stream of errors from the various processing stages.
    """

    def __init__(
        self,
        data_stream: DataStream,
        elastic_client: ElasticClient,
    ) -> None:
        """
        Initialize the PublishState with an input Kafka notifications stream.

        Parameters
        ----------
        data_stream : DataStream
            The input stream of Kafka notifications.
        """
        self.data_stream = data_stream
        self.elastic_client = elastic_client

        # Initialize the validation stage for input Kafka notifications.
        self.input_validation = ValidateKafkaNotifications(self.data_stream)

        # Initialize the stage for retrieving the previous entity versions from a database.
        self.previous_entity_retrieval = GetPreviousEntity(
            self.input_validation.main,
            elastic_client,
        )

        # Initialize the stage for preparing the validated notifications for indexing.
        self.index_preparation = PrepareNotificationToIndex(
            self.input_validation.main,
        )

        # Aggregate the errors from the various processing stages.
        self.errors = self.input_validation.errors.union(
            self.previous_entity_retrieval.errors,
        )
