import json
import logging
from collections.abc import Callable

from elasticsearch import ApiError, Elasticsearch
from m4i_atlas_core import AtlasChangeMessage, Entity, EntityAuditAction, get_entity_type_by_type_name
from pyflink.datastream import DataStream, MapFunction, OutputTag, RuntimeContext

from flink_tasks import AtlasChangeMessageWithPreviousVersion

ElasticsearchFactory = Callable[[], Elasticsearch]

ELASTICSEARCH_ERROR = OutputTag("elastic_error")
NO_ENTITY_ERROR = OutputTag("no_entity")
NO_PREVIOUS_ENTITY_ERROR = OutputTag("no_previous_entity")


class ElasticPreviousStateRetrieveError(Exception):
    """Exception raised for errors in the retrieval of previous state from ElasticSearch."""

    def __init__(self, guid: str, creation_time: int) -> None:
        """
        Initialize the ElasticPreviousStateRetrieveError exception.

        Parameters
        ----------
        guid : str
            The GUID for which the retrieval of previous state failed.
        creation_time : int
            The creation time for which the retrieval of previous state failed.
        """
        message = f"Failed to retrieve pervious state for guid {guid} and time {creation_time}"
        super().__init__(message)

        self.guid = guid
        self.creation_time = creation_time


class GetPreviousEntityFunction(MapFunction):
    """
    A custom `MapFunction` to retrieve the previous version of an entity from Elasticsearch.

    This function communicates with an Elasticsearch instance to obtain the previous version
    of an entity based on its GUID and creation time.

    Attributes
    ----------
    elastic_factory : ElasticClient
        Custom client for querying the desired Elasticsearch index.
    index_name : str
        The name of the index in Elasticsearch to which data is synchronized.
    """

    def __init__(self, elastic_factory: ElasticsearchFactory, index_name: str) -> None:
        """
        Initialize the `GetPreviousEntityFunction` instance.

        Parameters
        ----------
        elastic_factory : ElasticsearchFactory
            A factory function for creating Elasticsearch clients.
        """
        super().__init__()
        self.elastic_factory = elastic_factory
        self.index_name = index_name

    def open(self, context: RuntimeContext) -> None:  # noqa: ARG002
        """
        Initialize the Elasticsearch client.

        Parameters
        ----------
        context : RuntimeContext
            The context for this operator.
        """
        self.elasticsearch = self.elastic_factory()

    def map(
        self,
        value: AtlasChangeMessage,
    ) -> AtlasChangeMessageWithPreviousVersion | tuple[OutputTag, Exception]:
        """
        Map function to retrieve the previous version of an entity.

        Parameters
        ----------
        value : ValidatedInput
            The input message containing the entity to lookup.

        Returns
        -------
        AtlasChangeMessageWithPreviousVersion or tuple[OutputTag, Exception]
            The enriched message with the previous entity version or an error tuple.
        """
        entity = value.message.entity

        if entity is None:
            logging.error("Entity is required for lookup: %s", value)
            return NO_ENTITY_ERROR, ValueError("Entity is required for lookup")

        result = AtlasChangeMessageWithPreviousVersion(
            previous_version=None,
            version=value.version,
            message=value.message,
            msg_creation_time=value.msg_creation_time,
            msg_compression_kind=value.msg_compression_kind,
            msg_split_idx=value.msg_split_idx,
            msg_split_count=value.msg_split_count,
            msg_source_ip=value.msg_source_ip,
            msg_created_by=value.msg_created_by,
            spooled=value.spooled,
        )

        if value.message.operation_type == EntityAuditAction.ENTITY_CREATE:
            return result

        msg_creation_time = value.msg_creation_time

        query = {
            "bool": {
                "filter": [
                    {
                        "match": {
                            "guid.keyword": entity.guid,
                        },
                    },
                    {
                        "range": {
                            "updateTime": {
                                "lte": msg_creation_time,
                            },
                        },
                    },
                ],
            },
        }

        sort = {
            "updateTime": {"numeric_type": "long", "order": "desc"},
        }

        try:
            search_result = self.elasticsearch.search(
                index=self.index_name,
                query=query,
                sort=sort,
                size=1,
            )
        except ApiError as e:
            return ELASTICSEARCH_ERROR, ValueError(str(e))

        if search_result["hits"]["total"]["value"] == 0:
            logging.error(
                "No previous version found for entity %s at %s. (%s)",
                entity.guid,
                msg_creation_time,
                json.dumps(search_result),
            )
            return NO_PREVIOUS_ENTITY_ERROR, ValueError(
                f"No previous version found for entity {entity.guid} at {msg_creation_time}. \
                            ({json.dumps(search_result)})",
            )

        entity_type = get_entity_type_by_type_name(entity.type_name)

        result.previous_version = entity_type.from_dict(search_result["hits"]["hits"][0]["_source"])

        return result

    def close(self) -> None:
        """Close the Elasticsearch client."""
        self.elasticsearch.close()


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

    def __init__(
        self,
        input_stream: DataStream,
        elastic_factory: ElasticsearchFactory,
        index_name: str,
    ) -> None:
        """
        Initialize `GetPreviousEntity` with an input data stream.

        Parameters
        ----------
        input_stream : DataStream
            The input stream of validated notifications.
        elastic_factory : ElasticsearchFactory
            A factory function for creating Elasticsearch clients.
        index_name : str
            The name of the Elasticsearch index to query.
        """
        self.input_stream = input_stream
        self.elastic_factory = elastic_factory

        self.main = self.input_stream.map(
            GetPreviousEntityFunction(elastic_factory, index_name),
        ).name(
            "previous_entity_lookup",
        )

        self.elastic_errors = self.main.get_side_output(ELASTICSEARCH_ERROR).name("elastic_errors")

        self.no_previous_entity_errors = self.main.get_side_output(NO_PREVIOUS_ENTITY_ERROR).name(
            "no_previous_entity_errors",
        )

        self.errors = self.elastic_errors.union(
            self.no_previous_entity_errors,
        )
