from collections.abc import Callable
from functools import cached_property

from elasticsearch import ApiError, Elasticsearch

from .errors import ElasticPersistingError, ElasticPreviousStateRetrieveError
from .model import ElasticSearchEntity

ElasticsearchFactory = Callable[[], Elasticsearch]


class ElasticClient:
    """
    A client to interface with ElasticSearch for Aurelius Atlas-specific operations.

    The ElasticClient facilitates the fetching and indexing of atlas entities within an
    ElasticSearch index. This includes methods to fetch previous versions of atlas entities
    and to index new entity data.

    Attributes
    ----------
    elasticsearch_factory : ElasticsearchFactory
        A factory method that returns an instance of Elasticsearch when invoked.
    atlas_entities_index : str
        The ElasticSearch index name where atlas entities are stored.
    """

    def __init__(
        self,
        elasticsearch_factory: ElasticsearchFactory,
        atlas_entities_index: str = "atlas_entities_index",
    ) -> None:
        """
        Initialize an instance of ElasticClient.

        Parameters
        ----------
        elasticsearch_factory : ElasticsearchFactory
            A factory function that returns an Elasticsearch client instance.
        atlas_entities_index : str, optional
            The Elasticsearch index where atlas entities are stored.
            Default is "atlas_entities_index".
        """
        self.elasticsearch_factory = elasticsearch_factory
        self.atlas_entities_index = atlas_entities_index

    def get_previous_atlas_entity(
        self,
        entity_guid: str,
        creation_time: int,
    ) -> ElasticSearchEntity | None:
        """
        Retrieve the previous version of an entity using its GUID and creation time.

        This method performs a search query in the specified Elasticsearch index to find an entity
        matching the provided GUID that was created before the specified creation time.

        Parameters
        ----------
        entity_guid : str
            The unique identifier (GUID) of the entity to search for.
        creation_time : int
            The creation time to use for filtering the search results.

        Raises
        ------
        ElasticPreviousStateRetrieveError
            If an error occurs during the Elasticsearch query execution.

        Returns
        -------
        ElasticSearchEntity or None
            The found entity as a dictionary, or `None` if no matching entity is found.
        """
        query = {
            "bool": {
                "filter": [
                    {
                        "match": {
                            "body.guid.keyword": entity_guid,
                        },
                    },
                    {
                        "range": {
                            "msgCreationTime": {
                                "lt": creation_time,
                            },
                        },
                    },
                ],
            },
        }

        sort = {
            "msgCreationTime": {"numeric_type": "long", "order": "desc"},
        }

        try:
            result = self.elasticsearch.search(
                index=self.atlas_entities_index,
                query=query,
                sort=sort,
                size=1,
            )
        except ApiError as err:
            raise ElasticPreviousStateRetrieveError(
                entity_guid,
                creation_time,
                "publish_state",
            ) from err

        if result["hits"]["total"]["value"] == 0:
            return None

        return result["hits"]["hits"][0]["_source"]["body"]

    # there should be two separate instances: one creating the document and one writing the document
    # since writing the document is done in the kafka sink, the main functionality should be to
    # provide the key and the document to be published it is especially important that the document
    # created is a flat json document with strings as values the key should also be a string
    # where do we put the initialization for this index?
    def index_atlas_entity(
        self,
        entity_guid: str,
        msg_creation_time: int,
        event_time: int,
        atlas_entity: ElasticSearchEntity,  # this is wrong. This should be a KafkaNotification entity  # noqa: E501
    ) -> None:
        """
        Index an atlas entity in Elasticsearch.

        This method adds a new document to the specified Elasticsearch index, containing
        the provided atlas entity data and associated metadata.

        Parameters
        ----------
        entity_guid : str
            The unique identifier (GUID) of the atlas entity.
        msg_creation_time : int
            The message creation time associated with the atlas entity.
        event_time : int
            The event time associated with the atlas entity.
        atlas_entity : ElasticSearchEntity
            The atlas entity data to be indexed.

        Raises
        ------
        ElasticPersistingError
            If an error occurs while attempting to index the document in Elasticsearch.
        """
        doc_id = f"{entity_guid}_{msg_creation_time}"

        doc = {
            "msgCreationTime": msg_creation_time,
            "eventTime": event_time,
            "body": atlas_entity,
        }

        try:
            self.elasticsearch.index(
                index=self.atlas_entities_index,
                id=doc_id,
                document=doc,
            )
        except ApiError as err:
            raise ElasticPersistingError(doc_id, "publish_state") from err

    @cached_property
    def elasticsearch(self) -> Elasticsearch:
        """
        Provide a cached Elasticsearch client instance.

        This method uses the `elasticsearch_factory` to get an Elasticsearch instance and caches
        it for future use, ensuring that the same instance is reused across multiple calls.

        Returns
        -------
        Elasticsearch
            The Elasticsearch client instance.
        """
        return self.elasticsearch_factory()
