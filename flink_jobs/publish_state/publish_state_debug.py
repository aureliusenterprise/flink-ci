import debugpy
from pyflink.datastream import DataStream

from flink_jobs.elastic_client import ElasticClient

from .publish_state import PublishState


class DebugPublishState(PublishState):
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
        super().__init__(data_stream, elastic_client)

        # responsible for debuging on the jobmanager
        try:
            debugpy.listen(("localhost", 5678))
            debugpy.wait_for_client()  # blocks execution until client is attached
            debugpy.debug_this_thread()
            debugpy.trace_this_thread(True)
        except RuntimeError:
            pass
