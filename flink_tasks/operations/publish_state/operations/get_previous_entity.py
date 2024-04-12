import logging
from collections.abc import Callable
from dataclasses import dataclass

from elasticsearch import ApiError, Elasticsearch
from m4i_atlas_core import AtlasChangeMessage, Entity, EntityAuditAction, get_entity_type_by_type_name
from pyflink.datastream import DataStream, MapFunction, OutputTag, RuntimeContext

from flink_tasks import AtlasChangeMessageWithPreviousVersion
from flink_tasks.utils import ExponentialBackoff, retry

ElasticsearchFactory = Callable[[], Elasticsearch]

ELASTICSEARCH_ERROR = OutputTag("elastic_error")
NEWER_VERSION_ERROR = OutputTag("newer_version")
NO_ENTITY_ERROR = OutputTag("no_entity")
NO_PREVIOUS_ENTITY_ERROR = OutputTag("no_previous_entity")


@dataclass
class ElasticPreviousStateRetrieveError(Exception):
    """Exception raised for errors in the retrieval of previous state from ElasticSearch."""

    guid: str
    timestamp: int


class NoPreviousVersionError(ElasticPreviousStateRetrieveError):
    """Exception raised for errors in the retrieval of previous state from ElasticSearch."""

    def __str__(self) -> str:
        """Return a string representation of the error."""
        return f"No previous version found for entity {self.guid} at {self.timestamp}."


class NewerVersionError(ElasticPreviousStateRetrieveError):
    """Exception raised when a newer version of an entity is found in ElasticSearch."""

    def __str__(self) -> str:
        """Return a string representation of the error."""
        return f"Newer version found for entity {self.guid} at {self.timestamp}."


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

        logging.info(f"AtlasChangeMessage: {value}")

        msg_creation_time = value.msg_creation_time

        try:
            result.previous_version = self.get_previous_entity(entity, msg_creation_time)
        except ApiError as e:
            return ELASTICSEARCH_ERROR, ValueError(str(e))
        except NoPreviousVersionError as e:
            return NO_PREVIOUS_ENTITY_ERROR, e
        except NewerVersionError as e:
            return NEWER_VERSION_ERROR, e

        return result

    def close(self) -> None:
        """Close the Elasticsearch client."""
        self.elasticsearch.close()

    @retry(retry_strategy=ExponentialBackoff(), catch=(ApiError, NoPreviousVersionError))
    def get_previous_entity(
        self,
        current_version: Entity,
        timestamp: int,
    ) -> Entity:
        """Retrieve the previous version of an entity from ElasticSearch."""
        query = {
            "bool": {
                "filter": [
                    {
                        "match": {
                            "guid.keyword": current_version.guid,
                        },
                    },
                ],
            },
        }

        sort = {
            "updateTime": {"numeric_type": "long", "order": "desc"},
        }

        search_result = self.elasticsearch.search(
            index=self.index_name,
            query=query,
            sort=sort,
            size=1,
        )

        if search_result["hits"]["total"]["value"] == 0:
            logging.error("No previous version found for entity %s at %s.", current_version.guid, timestamp)
            raise NoPreviousVersionError(current_version.guid, timestamp)

        entity_type = get_entity_type_by_type_name(current_version.type_name)

        entity = entity_type.from_dict(search_result["hits"]["hits"][0]["_source"])
        if entity.update_time is not None and entity.update_time >= timestamp:
            logging.error(
                "Previous version found for entity %s at %s is newer than the current version.",
                current_version.guid,
                timestamp,
            )
            raise NewerVersionError(current_version.guid, timestamp)

        return entity


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
        self.newer_version_errors = self.main.get_side_output(NEWER_VERSION_ERROR).name("newer_version_errors")
        self.no_previous_entity_errors = self.main.get_side_output(NO_PREVIOUS_ENTITY_ERROR).name(
            "no_previous_entity_errors",
        )

        self.errors = self.elastic_errors.union(
            self.newer_version_errors,
            self.no_previous_entity_errors,
        )
