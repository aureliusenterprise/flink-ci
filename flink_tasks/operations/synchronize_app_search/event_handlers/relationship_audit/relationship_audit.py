from collections.abc import Generator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from flink_tasks import AppSearchDocument, EntityMessage, SynchronizeAppSearchError
from flink_tasks.utils import ExponentialBackoff, RetryError, retry

RELATIONSHIP_MAP = {
    "m4i_data_domain": "deriveddatadomain",
    "m4i_data_entity": "deriveddataentity",
    "m4i_data_attribute": "deriveddataattribute",
    "m4i_field": "derivedfield",
    "m4i_dataset": "deriveddataset",
    "m4i_collection": "derivedcollection",
    "m4i_system": "derivedsystem",
}


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


@retry(retry_strategy=ExponentialBackoff())
def get_current_document(guid: str, elastic: Elasticsearch, index_name: str) -> AppSearchDocument:
    """
    Get the document representing the entity with the given id from the Elasticsearch index.

    Parameters
    ----------
    guid : str
        The unique id of the entity.
    elastic : Elasticsearch
        The Elasticsearch client.
    index_name : str
        The name of the index.

    Returns
    -------
    AppSearchDocument
        The AppSearchDocument instance.
    """
    result = elastic.get(index=index_name, id=guid)

    if not result.body["found"]:
        raise AppSearchDocumentNotFoundError(guid)

    return AppSearchDocument.from_dict(result.body["_source"])


@retry(retry_strategy=ExponentialBackoff())
def get_related_documents(
    ids: list[str],
    elastic: Elasticsearch,
    index_name: str,
) -> list[AppSearchDocument]:
    """
    Get the related documents from the Elasticsearch index.

    Parameters
    ----------
    ids : list[str]
        The list of GUIDs of the related documents.
    elastic : Elasticsearch
        The Elasticsearch client.
    index_name : str
        The name of the index.

    Returns
    -------
    list[AppSearchDocument]
        A list of the related AppSearchDocument instances as they are retrieved from Elasticsearch.
    """
    query = {
        "query": {
            "terms": {
                "guid": ids,
            },
        },
    }

    results = [
        AppSearchDocument.from_dict(search_result["_source"])
        for search_result in scan(elastic, index=index_name, query=query)
    ]

    if len(results) != len(ids):
        message = "Some related documents were not found in the index"
        raise SynchronizeAppSearchError(message)

    return results


@retry(retry_strategy=ExponentialBackoff())
def get_child_documents(
    ids: list[str],
    elastic: Elasticsearch,
    index_name: str,
) -> Generator[AppSearchDocument, None, None]:
    """
    Get the related documents from the Elasticsearch index.

    Parameters
    ----------
    ids : list[str]
        The list of GUIDs of the direct child documents.
    elastic : Elasticsearch
        The Elasticsearch client.
    index_name : str
        The name of the index.

    Returns
    -------
    Generator[AppSearchDocument, None, None]
        Yields the related AppSearchDocument instances as they are retrieved from Elasticsearch.
    """
    query = {
        "query": {
            "terms": {
                "breadcrumbguid": ids,
            },
        },
    }

    for search_result in scan(elastic, index=index_name, query=query):
        yield AppSearchDocument.from_dict(search_result["_source"])


def handle_deleted_relationships(  # noqa: C901
    message: EntityMessage,
    document: AppSearchDocument,
    elastic: Elasticsearch,
    index_name: str,
    updated_documents: dict[str, AppSearchDocument],
) -> dict[str, AppSearchDocument]:
    """
    Handle the deleted relationships in the entity message.

    Parameters
    ----------
    message : EntityMessage
        The message containing the entity details and the relationships.
    document : AppSearchDocument
        The AppSearchDocument of the entity.
    elastic : Elasticsearch
        The Elasticsearch client.
    index_name : str
        The name of the index.
    updated_documents : dict[str, AppSearchDocument]
        The dictionary of updated AppSearchDocuments.
    """
    if message.deleted_relationships is None:
        return updated_documents

    deleted_relationships = [
        rel.guid for rels in message.deleted_relationships.values() for rel in rels if rel.guid is not None
    ]

    if not deleted_relationships:
        return updated_documents

    try:
        related_documents = get_related_documents(deleted_relationships, elastic, index_name)
    except RetryError as e:
        raise SynchronizeAppSearchError(message) from e

    for related_document in related_documents:
        if related_document.guid in updated_documents:
            related_document = updated_documents[related_document.guid]  # noqa: PLW2901

        field = RELATIONSHIP_MAP[related_document.typename]
        related_field = RELATIONSHIP_MAP[document.typename]

        guids: list[str] = getattr(document, f"{field}guid")
        names: list[str] = getattr(document, field)

        if related_document.guid in guids:
            idx = guids.index(related_document.guid)

            guids.pop(idx)
            names.pop(idx)

        related_guids: list[str] = getattr(related_document, f"{related_field}guid")
        related_names: list[str] = getattr(related_document, related_field)

        if document.guid in related_guids:
            idx = related_guids.index(document.guid)

            related_guids.pop(idx)
            related_names.pop(idx)

        updated_documents[related_document.guid] = related_document

    if message.old_value is None:
        return updated_documents

    breadcrumb_refs = {
        child.guid
        for child in message.old_value.get_children()
        if child.guid is not None and child.guid in deleted_relationships
    }

    # Add self to the breadcrumb refs in case of child -> parent relationship
    parents = set(message.old_value.get_parents())
    if any(parents.intersection(deleted_relationships)):
        breadcrumb_refs.add(document.guid)

    for child_document in get_child_documents(
        breadcrumb_refs,
        elastic,
        index_name,
    ):
        if child_document.guid in updated_documents:
            child_document = updated_documents[child_document.guid]  # noqa: PLW2901

        # Query guarantees that the breadcrumb includes the guid.
        idx = child_document.breadcrumbguid.index(document.guid)

        child_document.breadcrumbguid = child_document.breadcrumbguid[idx + 1 :]
        child_document.breadcrumbname = child_document.breadcrumbname[idx + 1 :]
        child_document.breadcrumbtype = child_document.breadcrumbtype[idx + 1 :]
        child_document.parentguid = child_document.breadcrumbguid[-1] if child_document.breadcrumbguid else None

    return updated_documents


def handle_inserted_relationships(  # noqa: C901
    message: EntityMessage,
    document: AppSearchDocument,
    elastic: Elasticsearch,
    index_name: str,
    updated_documents: dict[str, AppSearchDocument],
) -> dict[str, AppSearchDocument]:
    """
    Handle the inserted relationships in the entity message.

    Parameters
    ----------
    message : EntityMessage
        The message containing the entity details and the relationships.
    document : AppSearchDocument
        The AppSearchDocument of the entity.
    elastic : Elasticsearch
        The Elasticsearch client.
    index_name : str
        The name of the index.
    updated_documents : dict[str, AppSearchDocument]
        The dictionary of updated AppSearchDocuments.
    """
    if message.inserted_relationships is None:
        return updated_documents

    inserted_relationships = [
        rel.guid for rels in message.inserted_relationships.values() for rel in rels if rel.guid is not None
    ]

    if not inserted_relationships:
        return updated_documents

    try:
        related_documents = get_related_documents(inserted_relationships, elastic, index_name)
    except RetryError as e:
        raise SynchronizeAppSearchError(message) from e

    for related_document in related_documents:
        if related_document.guid in updated_documents:
            related_document = updated_documents[related_document.guid]  # noqa: PLW2901

        field = RELATIONSHIP_MAP[related_document.typename]
        related_field = RELATIONSHIP_MAP[document.typename]

        guids: list[str] = getattr(document, f"{field}guid")
        names: list[str] = getattr(document, field)

        if related_document.guid not in guids:
            guids.append(related_document.guid)
            names.append(related_document.name)

        related_guids: list[str] = getattr(related_document, f"{related_field}guid")
        related_names: list[str] = getattr(related_document, related_field)

        if document.guid not in related_guids:
            related_guids.append(document.guid)
            related_names.append(document.name)

        updated_documents[related_document.guid] = related_document

    if message.new_value is None:
        return updated_documents

    breadcrumb_refs = {
        child.guid
        for child in message.new_value.get_children()
        if child.guid is not None and child.guid in inserted_relationships
    }

    # Add self to the breadcrumb refs in case of child -> parent relationship
    parents = set(message.new_value.get_parents())
    if any(parents.intersection(inserted_relationships)):
        breadcrumb_refs.add(document.guid)

    for child_document in get_child_documents(
        list(breadcrumb_refs.intersection(inserted_relationships)),
        elastic,
        index_name,
    ):
        if child_document.guid in updated_documents:
            child_document = updated_documents[child_document.guid]  # noqa: PLW2901

        child_document.breadcrumbguid = [
            *document.breadcrumbguid,
            document.guid,
            *child_document.breadcrumbguid,
        ]

        child_document.breadcrumbname = [
            *document.breadcrumbname,
            document.name,
            *child_document.breadcrumbname,
        ]

        child_document.breadcrumbtype = [
            *document.breadcrumbtype,
            document.typename,
            *child_document.breadcrumbtype,
        ]

        child_document.parentguid = child_document.breadcrumbguid[-1] if child_document.breadcrumbguid else None

        updated_documents[child_document.guid] = child_document

    return updated_documents


def handle_relationship_audit(
    message: EntityMessage,
    elastic: Elasticsearch,
    index_name: str,
) -> list[AppSearchDocument]:
    """
    Handle the relationship audit event.

    Parameters
    ----------
    message : EntityMessage
        The message containing the entity details and the relationships.
    elastic : Elasticsearch
        The Elasticsearch client.
    index_name : str
        The name of the index.

    Returns
    -------
    list[AppSearchDocument]
        The list of updated AppSearchDocuments.
    """
    if not (message.inserted_relationships or message.deleted_relationships):
        return []

    document = get_current_document(message.guid, elastic, index_name)

    updated_documents: dict[str, AppSearchDocument] = {document.guid: document}

    updated_documents = handle_deleted_relationships(
        message,
        document,
        elastic,
        index_name,
        updated_documents,
    )

    updated_documents = handle_inserted_relationships(
        message,
        document,
        elastic,
        index_name,
        updated_documents,
    )

    updated_documents[document.guid].parentguid = document.breadcrumbguid[-1] if document.breadcrumbguid else None

    return list(updated_documents.values())
