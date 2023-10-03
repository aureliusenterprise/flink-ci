from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import ProcessFunction

from flink_jobs.elastic_client import (
    ElasticClient,
    ElasticPreviousStateRetrieveError,
)
from flink_jobs.job_runners import AtlasProcessFunction

from .model import ValidatedInput, ValidatedInputWithPreviousEntity

JOB_NAME = "publish_state"
DEAD_LETTER_TAG = OutputTag("dead_letter")


class GetPreviousEntity(AtlasProcessFunction):
    """
    A ProcessFunction for retrieving the previous version of an entity from Elasticsearch.

    Attributes
    ----------
    elastic_client : ElasticClient
        An instance of ElasticClient used to query Elasticsearch.
    """

    def __init__(self, input_stream: DataStream, elastic_client: ElasticClient) -> None:
        """
        Initialize the GetPreviousEntity object.

        Parameters
        ----------
        elastic_client : ElasticClient
            An instance of ElasticClient used to query Elasticsearch.
        """
        super().__init__(input_stream, JOB_NAME)

        self.elastic_client = elastic_client

        self.previous_entity_lookup = (
            self.input_stream
            .filter(lambda notif: notif)
            .process(self, Types.STRING())
            .name("previous_entity_lookup")
        )

        self.lookup_errors = self.previous_entity_lookup.get_side_output(
            DEAD_LETTER_TAG,
        )

    def process_element(
        self,
        value: ValidatedInput,
        _: ProcessFunction.Context | None = None,
    ) -> str | tuple[OutputTag, str]:
        """
        Process each element to retrieve the previous entity.

        Parameters
        ----------
        value : str
            The input value, as a serialized JSON string.
        _ : ProcessFunction.Context, optional
            The Flink processor context, by default None. Not used by this method.

        Returns
        -------
        str | tuple[OutputTag, str]
            The main output is the serialized JSON string of the kafka notification
            with the previous version. The side output is a tuple containing the error
            tag and the serialized error message.
        """
        entity_guid = value.entity.guid
        msg_creation_time = value.msg_creation_time

        try:
            previous_version = self.elastic_client.get_previous_atlas_entity(
                entity_guid=entity_guid,
                creation_time=msg_creation_time,
            )
        except ElasticPreviousStateRetrieveError as e:
            return self.create_exception_output(DEAD_LETTER_TAG, str(value), e)

        if previous_version is None:
            return self.create_error_output(
                DEAD_LETTER_TAG,
                str(value),
                "No previous version found for entity",
                "NoPreviousVersionError",
            )

        result = ValidatedInputWithPreviousEntity(
            value.entity,
            value.event_time,
            value.msg_creation_time,
            previous_version,
        )

        return result.to_json()
