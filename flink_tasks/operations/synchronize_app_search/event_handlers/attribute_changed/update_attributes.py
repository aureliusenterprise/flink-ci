import logging

from elasticsearch import Elasticsearch

from flink_tasks import AppSearchDocument, EntityMessage, SynchronizeAppSearchError
from flink_tasks.utils import ExponentialBackoff, retry

ATTRIBUTES_WHITELIST = {"definition", "email"}


class AppSearchDocumentNotFoundError(SynchronizeAppSearchError):
    """Exception raised when the AppSearchDocument is not found in the index."""

    def __init__(self, guid: str) -> None:
        """
        Initialize the exception.

        Parameters
        ----------
        guid : str
            The GUID of the entity for which the document was not found.
        """
        super().__init__(f"AppSearchDocument not found for entity {guid}")


class EntityDataNotProvidedError(SynchronizeAppSearchError):
    """Exception raised when the entity details are not provided in the message."""

    def __init__(self, guid: str) -> None:
        """
        Initialize the exception.

        Parameters
        ----------
        guid : str
            The GUID of the entity for which the data was not provided.
        """
        super().__init__(f"Entity data not provided for entity {guid}")


@retry(retry_strategy=ExponentialBackoff())
def get_document(
    guid: str,
    elastic: Elasticsearch,
    index_name: str,
) -> AppSearchDocument:
    """
    Retrieve an AppSearchDocument from the Elasticsearch index based on the GUID.

    Parameters
    ----------
    guid : str
        The GUID of the entity for which the document is to be retrieved.
    elastic : Elasticsearch
        The Elasticsearch client instance to interact with the Elasticsearch index.
    index_name : str
        The name of the Elasticsearch index where the entity is stored.

    Returns
    -------
    AppSearchDocument
        The AppSearchDocument instance corresponding to the entity.

    Raises
    ------
    AppSearchDocumentNotFoundError
        If the AppSearchDocument corresponding to the entity is not found in Elasticsearch.
    """
    result = elastic.get(index=index_name, id=guid)

    if not result.body["found"]:
        raise AppSearchDocumentNotFoundError(guid)

    return AppSearchDocument.from_dict(result.body["_source"])


def handle_update_attributes(
    message: EntityMessage,
    elastic: Elasticsearch,
    index_name: str,
) -> list[AppSearchDocument]:
    """
    Update specified attributes for an entity in the Elasticsearch index based on the EntityMessage.

    Parameters
    ----------
    message : EntityMessage
        The message containing the entity's update details.
    elastic : Elasticsearch
        The Elasticsearch client instance to interact with the Elasticsearch index.
    index_name : str
        The name of the Elasticsearch index where the entity is stored.

    Returns
    -------
    List[AppSearchDocument]
        A list containing the updated AppSearchDocument instance.

    Raises
    ------
    EntityDataNotProvidedError
        If the `new_value` attribute of the message is not provided.
    AppSearchDocumentNotFoundError
        If the AppSearchDocument corresponding to the entity is not found in Elasticsearch.

    Notes
    -----
    The function only updates attributes that are in the `ATTRIBUTES_WHITELIST` and have been
    either inserted or changed as indicated by the `EntityMessage`.
    """
    attributes_to_update = ATTRIBUTES_WHITELIST & (set(message.inserted_attributes) | set(message.changed_attributes))

    logging.info(f"handle_update_attributes - {message} - {attributes_to_update}")

    if len(attributes_to_update) == 0:
        return []

    entity_details = message.new_value

    if entity_details is None:
        raise EntityDataNotProvidedError(message.guid)

    result = get_document(message.guid, elastic, index_name)

    logging.info(f"handle_update_attributes - old - {result}")

    for attribute in attributes_to_update:
        value = getattr(entity_details.attributes, attribute)
        setattr(result, attribute, value)

    logging.info(f"handle_update_attributes - new - {result}")

    return [result]
