from elasticsearch import ApiError, Elasticsearch

from .errors import ElasticPersistingError, ElasticPreviousStateRetrieveError
from .model import ElasticSearchEntity


class ElasticClient:
    """
    ElasticClient provides queries for Aurelius Atlas-specific interaction with ElasticSearch.

    This class enables the retrieval and indexing of atlas entities within an ElasticSearch index,
    providing methods for fetching previous entity versions and indexing new entity data.

    Attributes
    ----------
    elasticsearch : Elasticsearch
        The ElasticSearch client instance used for executing queries.
    atlas_entities_index : str
        The name of the ElasticSearch index used for storing atlas entities.
    """

    def __init__(
        self,
        elasticsearch: Elasticsearch,
        atlas_entities_index: str = "atlas_entities_index",
    ) -> None:
        """
        Initialize the ElasticClient object.

        Parameters
        ----------
        elasticsearch : Elasticsearch
            The ElasticSearch client instance to be used.
        atlas_entities_index : str, optional
            The name of the target ElasticSearch index, by default "atlas_entities_index".
        """
        self.elasticsearch = elasticsearch
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
            ) from err

        if result["hits"]["total"]["value"] == 0:
            return None

        return result["hits"]["hits"][0]["_source"]["body"]

    def index_atlas_entity(
        self,
        entity_guid: str,
        msg_creation_time: int,
        event_time: int,
        atlas_entity: ElasticSearchEntity,
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
            raise ElasticPersistingError(doc_id) from err
