import logging
from collections.abc import Generator

from flink_tasks import AppSearchDocument, EntityMessage, SynchronizeAppSearchError
from flink_tasks.operations.synchronize_app_search.transaction import Transaction
from flink_tasks.utils import ExponentialBackoff, retry

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
    """Get the related documents from the Elasticsearch index."""
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
    """Get the related documents from the Elasticsearch index."""
    query = {
        "query": {
            "match": {
                "breadcrumbguid": guid,
            },
        },
    }

    logging.debug("Searching for grandchildren with breadcrumb containing id %s", guid)

    return transaction.find_many(query)


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


def delete_relationship(source: AppSearchDocument, target: AppSearchDocument) -> AppSearchDocument:
    """Insert a relationship between two entities in one direction."""
    relationship_name = RELATIONSHIP_MAP[target.typename]
    guids: list[str] = getattr(source, f"{relationship_name}guid")
    names: list[str] = getattr(source, relationship_name)

    if target.guid in guids:
        idx = guids.index(target.guid)

        guids.pop(idx)
        names.pop(idx)

        logging.info("Removed relationship %s for entity %s", target.guid, source.guid)
        logging.debug("Updated %s ids: %s", relationship_name, guids)
        logging.debug("Updated %s names: %s", relationship_name, names)
    else:
        logging.info("Relationship %s for entity %s not found", target.guid, source.guid)

    return source


def handle_deleted_relationships(
    message: EntityMessage,
    document: AppSearchDocument,
    transaction: Transaction,
) -> Transaction:
    """Handle the deleted relationships in the entity message."""
    if message.deleted_relationships is None:
        logging.warning("Deleted relationships not provided for entity %s", message.guid)
        return transaction

    if not any(message.deleted_relationships.values()):
        logging.info("No relationships to delete for entity %s", message.guid)
        return transaction

    if message.old_value is None:
        logging.error("Entity data not provided for entity %s", message.guid)
        raise EntityDataNotProvidedError(message.guid)

    """
    Filter out child-parent relationships.

    Every relationship change emits two events. One for the parent and one for the child. Only the parent to child
    relationship should be handled to avoid duplicate changes and inconsistencies due to race conditions.
    """
    children = [child.guid for child in message.old_value.get_children() if child.guid is not None]
    children_to_delete = [
        entity.guid for rel in message.deleted_relationships.values() for entity in rel if entity.guid in children
    ]

    logging.debug("Relationships to delete: %s", children_to_delete)

    if not children_to_delete:
        logging.info("No relationships to delete for entity %s", message.guid)
        return transaction

    child_documents = find_children(children_to_delete, transaction)

    for child_document in child_documents:
        delete_relationship(document, child_document)
        delete_relationship(child_document, document)

        set_parent(child_document, None)

        transaction.commit(child_document)

        for descendant in find_descendants(child_document.guid, transaction):
            set_parent(descendant, child_document, indirect=True)

            transaction.commit(descendant)

    return transaction


def insert_relationship(source: AppSearchDocument, target: AppSearchDocument) -> AppSearchDocument:
    """Insert a relationship between two entities in one direction."""
    relationship_name = RELATIONSHIP_MAP[target.typename]
    guids: list[str] = getattr(source, f"{relationship_name}guid")
    names: list[str] = getattr(source, relationship_name)

    if target.guid not in guids:
        guids.append(target.guid)
        names.append(target.name)

        logging.info("Inserted relationship %s for entity %s", target.guid, source.guid)
        logging.debug("Updated %s ids: %s", relationship_name, guids)
        logging.debug("Updated %s names: %s", relationship_name, names)

    else:
        logging.info("Relationship %s for entity %s already exists", target.guid, source.guid)

    return source


def handle_inserted_relationships(
    message: EntityMessage,
    document: AppSearchDocument,
    transaction: Transaction,
) -> Transaction:
    """Handle the inserted relationships in the entity message."""
    if message.inserted_relationships is None:
        logging.warning("Inserted relationships not provided for entity %s", message.guid)
        return transaction

    if not any(message.inserted_relationships.values()):
        logging.info("No relationships to insert for entity %s", message.guid)
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
    children_to_insert = [
        entity.guid for rel in message.inserted_relationships.values() for entity in rel if entity.guid in children
    ]

    logging.debug("Relationships to insert: %s", children_to_insert)

    if not children_to_insert:
        logging.info("No relationships to insert for entity %s", message.guid)
        return transaction

    child_documents = find_children(children_to_insert, transaction)

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
    transaction: Transaction
        The transaction instance managing the updates to the AppSearch index.

    Returns
    -------
    Transaction
        The updated transaction instance.
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

    transaction.commit(document)

    return transaction
