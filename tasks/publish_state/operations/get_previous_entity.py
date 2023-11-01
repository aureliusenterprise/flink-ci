from pyflink.datastream import DataStream, MapFunction, OutputTag

from tasks.elastic_client import (
    ElasticClient,
    ElasticPreviousStateRetrieveError,
)
from tasks.publish_state import ValidatedInput, ValidatedInputWithPreviousEntity

ELASTICSEARCH_ERROR = OutputTag("elastic_error")
NO_PREVIOUS_ENTITY_ERROR = OutputTag("no_previous_entity")

class GetPreviousEntityFunction(MapFunction):
    """
    A custom `MapFunction` to retrieve the previous version of an entity from Elasticsearch.

    This function communicates with an Elasticsearch instance to obtain the previous version
    of an entity based on its GUID and creation time.

    Attributes
    ----------
    elastic_client : ElasticClient
        Custom client for querying the desired Elasticsearch index.
    """

    def __init__(self, elastic_client: ElasticClient) -> None:
        """
        Initialize the `GetPreviousEntityFunction` instance.

        Parameters
        ----------
        elastic_client : ElasticClient
            Custom client for querying the desired Elasticsearch index.
        """
        super().__init__()
        self.elastic_client = elastic_client

    def map( # noqa: A003
        self,
        value: ValidatedInput,
    ) -> ValidatedInputWithPreviousEntity | tuple[OutputTag, Exception]:
        """
        Map function to retrieve the previous version of an entity.

        Parameters
        ----------
        value : ValidatedInput
            The input message containing the entity to lookup.

        Returns
        -------
        ValidatedInputWithPreviousEntity or tuple[OutputTag, Exception]
            The enriched message with the previous entity version or an error tuple.
        """
        entity_guid = value.entity.guid
        msg_creation_time = value.msg_creation_time

        try:
            previous_version = self.elastic_client.get_previous_atlas_entity(
                entity_guid=entity_guid,
                creation_time=msg_creation_time,
            )
        except ElasticPreviousStateRetrieveError as e:
            return ELASTICSEARCH_ERROR, e

        if previous_version is None:
            return NO_PREVIOUS_ENTITY_ERROR, ValueError(
                f"No previous version found for entity {entity_guid}",
            )

        return ValidatedInputWithPreviousEntity(
            value.entity,
            value.event_time,
            value.msg_creation_time,
            previous_version,
        )

class GetPreviousEntity:
    """
    A class that sets up the Flink data stream for retrieving previous entity versions.

    This class initializes the data stream and applies the `GetPreviousEntityFunction`
    to fetch the previous entity versions. It organizes the output into `main`,
    `elastic_errors`, `no_previous_entity_errors`, and `errors` streams.

    Attributes
    ----------
    input_stream : DataStream
        The input stream of validated messages.
    main : DataStream
        The main output stream containing messages with previous entities.
    elastic_errors : DataStream
        The side output stream for messages that encountered Elasticsearch errors.
    no_previous_entity_errors : DataStream
        The side output stream for messages without previous entities.
    errors : DataStream
        The union of elastic_errors and no_previous_entity_errors.
    """

    def __init__(self, input_stream: DataStream, elastic_client: ElasticClient) -> None:
        """
        Initialize `GetPreviousEntity` with an input data stream.

        Parameters
        ----------
        input_stream : DataStream
            The input stream of validated notifications.
        """
        self.input_stream = input_stream
        self.elastic_client = elastic_client

        self.main = (
            self.input_stream
            .map(GetPreviousEntityFunction(elastic_client))
            .name("previous_entity_lookup")
        )

        self.elastic_errors = (
            self.main
            .get_side_output(ELASTICSEARCH_ERROR)
            .name("elastic_errors")
        )

        self.no_previous_entity_errors = (
            self.main
            .get_side_output(NO_PREVIOUS_ENTITY_ERROR)
            .name("no_previous_entity_errors")
        )

        self.errors = self.elastic_errors.union(
            self.no_previous_entity_errors,
        )
