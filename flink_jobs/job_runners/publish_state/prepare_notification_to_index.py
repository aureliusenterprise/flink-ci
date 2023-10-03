

from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import ProcessFunction

from flink_jobs.job_runners import AtlasProcessFunction

from .model import EntityVersion, ValidatedInput

JOB_NAME = "publish_state"


class PrepareNotificationToIndex(AtlasProcessFunction):
    """
    A ProcessFunction to prepare notifications for indexing in Elasticsearch.

    This class extends AtlasProcessFunction and processes each element to prepare the
    notification by validating and formatting the entity for Elasticsearch storage.
    """

    def __init__(self, input_stream: DataStream) -> None:
        """
        Initialize the PrepareNotificationToIndex object.

        Parameters
        ----------
        error_tag : OutputTag
            An OutputTag to use for error output.
        """
        super().__init__(input_stream, JOB_NAME)

        self.prepared_documents = (
            self.input_stream
            .filter(lambda notif: notif)
            .process(self, Types.STRING())
            .name("index_preparation")
        )

    def process_element(
        self,
        value: ValidatedInput,
        _: ProcessFunction.Context | None = None,
    ) -> str | tuple[OutputTag, str]:
        """
        Prepare the given notification for indexing in Elasticsearch.

        Parameters
        ----------
        value : KafkaNotification
            The input value, a serialized JSON string representing the Kafka notification.
        _ : ProcessFunction.Context, optional
            The context, ignored in this function. Default is None.

        Returns
        -------
        str | tuple[OutputTag, str]
            If processing is successful, return a JSON string prepared for Elasticsearch.
            If there is an error, it returns a tuple containing the error tag and error message.
        """
        msg_creation_time = value.msg_creation_time
        event_time = value.event_time

        doc_id = f"{value.entity.guid}_{msg_creation_time}"

        result = EntityVersion(
            value.entity,
            doc_id,
            event_time,
            msg_creation_time,
        )

        return result.to_json()
