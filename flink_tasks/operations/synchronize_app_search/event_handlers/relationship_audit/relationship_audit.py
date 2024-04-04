import logging
from collections.abc import Generator

from flink_tasks import AppSearchDocument, EntityMessage, SynchronizeAppSearchError
from flink_tasks.operations.synchronize_app_search.transaction import Transaction
from flink_tasks.utils import ExponentialBackoff, RetryError, retry

RELATIONSHIP_MAP = {
    "m4i_data_domain": "deriveddatadomain",
    "m4i_data_entity": "deriveddataentity",
    "m4i_data_attribute": "deriveddataattribute",
    "m4i_field": "derivedfield",
    "m4i_dataset": "deriveddataset",
    "m4i_collection": "derivedcollection",
    "m4i_system": "derivedsystem",
    "m4i_person": "derivedperson",
    "m4i_generic_process": "derivedprocess",
}


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
def find_children(
    ids: list[str],
    transaction: Transaction,
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
            "match": {
                "guid": " ".join(ids),
            },
        },
    }

    logging.debug("Searching for related documents with ids %s", ids)

    results = list(transaction.find_many(query))

    if len(results) != len(ids):
        message = "Some related documents were not found in the index"
        logging.error(message)
        raise SynchronizeAppSearchError(message)

    return results


@retry(retry_strategy=ExponentialBackoff())
def find_descendants(guid: str, transaction: Transaction) -> Generator[AppSearchDocument, None, None]:
    """
    Get the related documents from the Elasticsearch index.

    Parameters
    ----------
    guid : str
        The id of a child document
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
            "match": {
                "breadcrumbguid": guid,
            },
        },
    }

    logging.debug("Searching for grandchildren with breadcrumb containing id %s", guid)

    return transaction.find_many(query)


def handle_deleted_relationships(  # noqa: C901
    message: EntityMessage,
    document: AppSearchDocument,
    transaction: Transaction,
) -> Transaction:
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
        logging.warning("Deleted relationships not provided for entity %s", message.guid)
        return transaction

    if message.old_value is None:
        logging.error("Entity data not provided for entity %s", message.guid)
        raise EntityDataNotProvidedError(message.guid)

    parents = list(message.old_value.get_parents())

    deleted_relationships = [
        rel.guid
        for rels in message.deleted_relationships.values()
        for rel in rels
        if rel.guid is not None and rel.guid not in parents
    ]

    logging.debug("Relationships to delete: %s", deleted_relationships)

    if not deleted_relationships:
        logging.info("No relationships to delete for entity %s", message.guid)
        return transaction

    try:
        related_documents = find_children(deleted_relationships, transaction)
    except RetryError as e:
        logging.exception("Error retrieving related documents for entity %s", message.guid)
        raise SynchronizeAppSearchError(message) from e

    for related_document in related_documents:
        field = RELATIONSHIP_MAP[related_document.typename]
        related_field = RELATIONSHIP_MAP[document.typename]

        guids: list[str] = getattr(document, f"{field}guid")
        names: list[str] = getattr(document, field)

        if related_document.guid in guids:
            idx = guids.index(related_document.guid)

            guids.pop(idx)
            names.pop(idx)

        logging.info("Deleted relationship %s for entity %s", document.typename, document.guid)
        logging.debug("Updated ids: %s", guids)
        logging.debug("Updated names: %s", names)

        related_guids: list[str] = getattr(related_document, f"{related_field}guid")
        related_names: list[str] = getattr(related_document, related_field)

        if document.guid in related_guids:
            idx = related_guids.index(document.guid)

            related_guids.pop(idx)
            related_names.pop(idx)

        logging.info("Deleted relationship %s for entity %s", related_document.typename, related_document.guid)
        logging.debug("Updated ids: %s", related_guids)
        logging.debug("Updated names: %s", related_names)

        transaction.commit(related_document)

    breadcrumb_refs = {
        child.guid
        for child in message.old_value.get_children()
        if child.guid is not None and child.guid in deleted_relationships
    }

    # Add self to the breadcrumb refs in case of child -> parent relationship
    parents = {ref.guid for ref in message.old_value.get_parents() if ref.guid is not None}
    remaining_parent_relationships = list(parents.difference(deleted_relationships))

    if len(remaining_parent_relationships) < len(parents):
        # Add self to the breadcrumb refs in case of child -> parent relationship
        breadcrumb_refs.add(document.guid)

        parent_id = remaining_parent_relationships[0] if remaining_parent_relationships else None

        if parent_id is not None:
            parent_document = transaction.find_one(parent_id)

            if parent_document is not None:
                document.breadcrumbguid = [
                    *parent_document.breadcrumbguid,
                    parent_document.guid,
                ]

                document.breadcrumbname = [
                    *parent_document.breadcrumbname,
                    parent_document.name,
                ]

                document.breadcrumbtype = [
                    *parent_document.breadcrumbtype,
                    parent_document.typename,
                ]

                document.parentguid = parent_document.guid

                logging.info("Set parent of entity %s to %s", document.guid, parent_document.guid)
                logging.debug("Breadcrumb GUID: %s", document.breadcrumbguid)
                logging.debug("Breadcrumb Name: %s", document.breadcrumbname)
                logging.debug("Breadcrumb Type: %s", document.breadcrumbtype)

    elif len(remaining_parent_relationships) == 0:
        # Add self to the breadcrumb refs in case of child -> parent relationship
        breadcrumb_refs.add(document.guid)

        document.breadcrumbguid = []
        document.breadcrumbname = []
        document.breadcrumbtype = []

        document.parentguid = None

        logging.info("Removed parent of entity %s", document.guid)

    immediate_children = {
        child.guid
        for child in message.old_value.get_children()
        if child.guid is not None and child.guid in deleted_relationships
    }

    # delete immediate children relation
    for child_guid in immediate_children:
        child_document = transaction.find_one(child_guid)

        if child_document is None:
            logging.error("Document not found for entity %s", child_guid)
            continue

        # Query guarantees that the breadcrumb includes the guid.
        idx = child_document.breadcrumbguid.index(document.guid)

        child_document.breadcrumbguid = child_document.breadcrumbguid[idx + 1 :]
        child_document.breadcrumbname = child_document.breadcrumbname[idx + 1 :]
        child_document.breadcrumbtype = child_document.breadcrumbtype[idx + 1 :]
        child_document.parentguid = child_document.breadcrumbguid[-1] if child_document.breadcrumbguid else None

        logging.info("Set parent relationship of entity %s to %s", child_document.guid, child_document.parentguid)
        logging.debug("Breadcrumb GUID: %s", child_document.breadcrumbguid)
        logging.debug("Breadcrumb Name: %s", child_document.breadcrumbname)
        logging.debug("Breadcrumb Type: %s", child_document.breadcrumbtype)

        transaction.commit(child_document)

    for child_document in find_descendants(list(breadcrumb_refs), transaction):
        # Query guarantees that the breadcrumb includes the guid.
        idx = child_document.breadcrumbguid.index(document.guid)

        child_document.breadcrumbguid = child_document.breadcrumbguid[idx + 1 :]
        child_document.breadcrumbname = child_document.breadcrumbname[idx + 1 :]
        child_document.breadcrumbtype = child_document.breadcrumbtype[idx + 1 :]
        child_document.parentguid = child_document.breadcrumbguid[-1] if child_document.breadcrumbguid else None

        logging.info("Set parent relationship of entity %s to %s", child_document.guid, child_document.parentguid)
        logging.debug("Breadcrumb GUID: %s", child_document.breadcrumbguid)
        logging.debug("Breadcrumb Name: %s", child_document.breadcrumbname)
        logging.debug("Breadcrumb Type: %s", child_document.breadcrumbtype)

        transaction.commit(child_document)

    return transaction


def insert_relationship(source: AppSearchDocument, target: AppSearchDocument) -> None:
    """Insert a relationship between two entities in one direction."""
    relationship_name = RELATIONSHIP_MAP[target.typename]
    guids: list[str] = getattr(source, f"{relationship_name}guid")
    names: list[str] = getattr(source, relationship_name)

    if target.guid not in guids:
        guids.append(target.guid)
        names.append(target.name)

        logging.info("Inserted relationship %s for entity %s", target.typename, source.guid)
        logging.debug("Updated ids: %s", guids)
        logging.debug("Updated names: %s", names)


def set_parent(
    child: AppSearchDocument,
    parent: AppSearchDocument | None,
    *,
    indirect: bool = False,
) -> AppSearchDocument:
    """
    Set or update the parent relationship for a child document, adjusting its breadcrumb trail.

    If a parent is provided, the child document's breadcrumb trail is updated to reflect the new parentage.
    If no parent is provided (parent is None), the child's breadcrumb trail is cleared, indicating it has no parent.

    Parameters
    ----------
    child : AppSearchDocument
        The child document whose parent relationship is to be updated.
    parent : AppSearchDocument | None, optional
        The new parent document, or None if the child should have no parent.
    indirect : bool, optional
        Whether the parent-child relationship is indirect (e.g. grandparent to grandchild). Defaults to False.

    Returns
    -------
    AppSearchDocument
        The child document with updated parent relationship and breadcrumb trail.
    """
    if parent is None:
        logging.info("Removing parent %s for child entity %s.", child.parentguid, child.guid)
        child.breadcrumbguid = []
        child.breadcrumbname = []
        child.breadcrumbtype = []
        child.parentguid = None

    elif indirect and parent.guid in child.breadcrumbguid:
        logging.info("Updating indirect parent for child entity %s to %s.", child.guid, parent.guid)
        idx = child.breadcrumbguid.index(parent.guid)

        child.breadcrumbguid = [*parent.breadcrumbguid, parent.guid, *child.breadcrumbguid[idx + 1 :]]
        child.breadcrumbname = [*parent.breadcrumbname, parent.name, *child.breadcrumbname[idx + 1 :]]
        child.breadcrumbtype = [*parent.breadcrumbtype, parent.typename, *child.breadcrumbtype[idx + 1 :]]

        child.parentguid = child.breadcrumbguid[-1]

    else:
        logging.info("Updating parent for child entity %s to %s.", child.guid, parent.guid)
        child.breadcrumbguid = [*parent.breadcrumbguid, parent.guid]
        child.breadcrumbname = [*parent.breadcrumbname, parent.name]
        child.breadcrumbtype = [*parent.breadcrumbtype, parent.typename]

        child.parentguid = parent.guid

    logging.info("Set parent relationship of entity %s to %s", child.guid, child.parentguid)
    logging.debug("Breadcrumb GUID: %s", child.breadcrumbguid)
    logging.debug("Breadcrumb Name: %s", child.breadcrumbname)
    logging.debug("Breadcrumb Type: %s", child.breadcrumbtype)

    return child


def handle_inserted_relationships(
    message: EntityMessage,
    document: AppSearchDocument,
    transaction: Transaction,
) -> Transaction:
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
        logging.warning("Inserted relationships not provided for entity %s", message.guid)
        return transaction

    if message.new_value is None:
        logging.error("Entity data not provided for entity %s", message.guid)
        raise EntityDataNotProvidedError(message.guid)

    """
    Filter out child-parent relationships.

    Every relationship change emits two events. One for the parent and one for the child. Only the parent to child
    relationship should be handled to avoid duplicate changes and inconsistencies due to race conditions.
    """
    children = [child.guid for child in message.new_value.get_children() if child.guid is not None]
    children_to_update = [
        entity.guid for rel in message.inserted_relationships.values() for entity in rel if entity.guid in children
    ]

    logging.debug("Relationships to insert: %s", children_to_update)

    if not children_to_update:
        logging.info("No relationships to insert for entity %s", message.guid)
        return transaction

    child_documents = find_children(children_to_update, transaction)

    for child in child_documents:
        insert_relationship(document, child)
        insert_relationship(child, document)

        set_parent(child, document)

        transaction.commit(child)

        for descendant in find_descendants(child.guid, transaction):
            set_parent(descendant, child, indirect=True)

            transaction.commit(descendant)

    return transaction


def handle_relationship_audit(message: EntityMessage, transaction: Transaction) -> Transaction:
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
        logging.info("No relationships to update for entity %s", message.guid)
        return transaction

    document = transaction.find_one(message.guid)

    if document is None:
        logging.error("Document not found for entity %s", message.guid)
        return transaction

    transaction = handle_deleted_relationships(
        message,
        document,
        transaction,
    )

    transaction = handle_inserted_relationships(
        message,
        document,
        transaction,
    )

    document.parentguid = document.breadcrumbguid[-1] if document.breadcrumbguid else None

    transaction.commit(document)

    return transaction
