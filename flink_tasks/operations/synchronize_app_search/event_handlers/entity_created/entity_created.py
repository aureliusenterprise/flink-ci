from collections.abc import Generator
from typing import Any, cast

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from m4i_atlas_core import Entity

from flink_tasks import AppSearchDocument, EntityMessage, SynchronizeAppSearchError
from flink_tasks.utils import ExponentialBackoff, retry

RELATIONSHIP_MAP = {
    "m4i_data_domain": ["deriveddatadomain"],
    "m4i_data_entity": ["deriveddataentity"],
    "m4i_data_attribute": ["deriveddataattribute"],
    "m4i_field": ["derivedfield"],
    "m4i_dataset": ["deriveddataset"],
    "m4i_collection": ["derivedcollection"],
    "m4i_system": ["derivedsystem"],
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
def get_documents(
    query: dict,
    elastic: Elasticsearch,
    index_name: str,
) -> Generator[AppSearchDocument, None, None]:
    """
    Yield AppSearchDocument objects from Elasticsearch based on the given query.

    Parameters
    ----------
    query : dict
        The Elasticsearch query used to fetch documents.
    elastic : Elasticsearch
        The Elasticsearch client instance.
    index_name : str
        The name of the index in Elasticsearch to query.

    Yields
    ------
    Generator[AppSearchDocument, None, None]
        Yields AppSearchDocument instances as they are retrieved from Elasticsearch.
    """
    for result in scan(elastic, index=index_name, query=query):
        yield AppSearchDocument.from_dict(result["_source"])


def get_breadcrumbs_of_entity(
    input_entity: Entity,
    elastic: Elasticsearch,
    index_name: str,
) -> dict[str, Any]:
    """
    Extract parent entity breadcrumbs based on the provided input_entity.

    Parameters
    ----------
    input_entity : Entity
        The Entity instance for which breadcrumbs need to be updated.
    elastic : Elasticsearch
        The Elasticsearch client for database interaction.
    index_name : str
        The name of the index in Elasticsearch to query.

    Returns
    -------
    dict[str, Any]
        A dict containing updated breadcrumb details - name, GUID, and type.
    """
    attributes: dict[str, Any] = {}
    # set default values
    attributes.update({"breadcrumbname": [], "breadcrumbguid": [], "breadcrumbtype": []})
    # Get the first parent of the entity
    parents = [x.guid for x in input_entity.get_parents()]
    if parents:
        # Look up breadcrumbs of parents
        query = {"query": {"match": {"guid": parents[0]}}}

        for document in get_documents(query, elastic, index_name):
            attributes.update(
                {
                    "breadcrumbname": [*document.breadcrumbname, document.name],
                    "breadcrumbguid": [*document.breadcrumbguid, document.guid],
                    "breadcrumbtype": [*document.breadcrumbtype, document.typename],
                },
            )

    return attributes


def create_derived_relations(
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
    referenced: list[str | None],
) -> Generator[AppSearchDocument, None, None]:
    """
    Update existing `AppSearchDocument`s that represent entities related to the provided entity.

    Parameters
    ----------
    entity_details: Entity
        Details of the main entity for which derived relations are created.
    elastic: Elasticsearch
        Elasticsearch client for querying documents.
    index_name: str
        Name of the Elasticsearch index containing the relevant documents.
    referenced : dict[str, list]
        GUIDs representing the related entities.

    Returns
    -------
    Generator[AppSearchDocument, None, None]
        A generator yielding AppSearchDocument objects representing the derived relations.
    """
    # Get all related entities of the main entity
    query = {"query": {"match": {"guid": ' '.join(referenced)}}}

    for document in get_documents(query, elastic, index_name):
        for key in RELATIONSHIP_MAP[entity_details.type_name]:
            # The query guarantees that the relationship attributes are present in the document.
            # No need for try/except block to handle a potential KeyError.
            guids: list[str] = getattr(document, key + "guid")
            names: list[str] = getattr(document, key)
            # Update guid and name with a fallback value to qualifiedName
            qualified_name = getattr(entity_details.attributes, "qualified_name", "")
            name = getattr(entity_details.attributes, "name", qualified_name)
            # Append to the list
            guids.append(entity_details.guid)
            names.append(name)
        yield document


def update_children_breadcrumb(
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
    attr: dict[str, Any],
) -> Generator[AppSearchDocument, None, None]:
    """
    Update the breadcrumb of the created entity's children.

    Parameters
    ----------
    entity_details: Entity
        Details of the main entity for which derived relations are created.
    elastic: Elasticsearch
        Elasticsearch client for querying documents.
    index_name: str
        Name of the Elasticsearch index containing the relevant documents.
    attr: dict[str, Any]
        A Dict object containing the breadcrumb information of the main entity

    Returns
    -------
    Generator[AppSearchDocument, None, None]
        A generator yielding AppSearchDocument objects representing the updated children.
    """
    # A list of children of the main entity
    list_of_children = [x.guid for x in entity_details.get_children()]
    # Find all documents that reference immediate children of the main entity in their breadcrumb
    query = {"query": {"match": {"breadcrumbguid": ' '.join(list_of_children)}}}
    # Get name of the main entity
    qualified_name = getattr(entity_details.attributes, "qualified_name", "")
    name = getattr(entity_details.attributes, "name", qualified_name)
    # Set the breadcrumbs of all children
    for document in get_documents(query, elastic, index_name):
        document.breadcrumbname = [*attr["breadcrumbname"], name, *document.breadcrumbname]
        document.breadcrumbguid = [
            *attr["breadcrumbguid"],
            entity_details.guid,
            *document.breadcrumbguid,
        ]
        document.breadcrumbtype = [
            *attr["breadcrumbtype"],
            entity_details.type_name,
            *document.breadcrumbtype,
        ]
        yield document


def update_existing_documents(
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
    breadcrumbs: dict,
) -> tuple[list[AppSearchDocument], dict[str, Any]]:
    """
    Update the children and related documents of the main entity.

    Parameters
    ----------
    entity_details: Entity
        Details of the main entity for which derived relations are created.
    elastic: Elasticsearch
        Elasticsearch client for querying documents.
    index_name: str
        Name of the Elasticsearch index containing the relevant documents.
    breadcrumbs: tuple[list[AppSearchDocument], dict[str, list]]
        It returns a combined list of related entities and child entities,
        secondly it returns the names, and guids of the related entities.

    Returns
    -------
    Generator[AppSearchDocument, None, None]
        A generator yielding AppSearchDocument objects representing the updated children.
    """
    relationships = [ref.guid for ref in entity_details.get_referred_entities()]
    # Query related entities
    related = list(create_derived_relations(entity_details, elastic, index_name, relationships))
    # Create a dictionary of {guid: related documents}
    related_dict = {doc.guid: doc for doc in related}
    referenced_guids, referenced_names = {}, {}
    for ref in related:
        keys = RELATIONSHIP_MAP[ref.typename]
        for key in keys:
            # Referenced entity's name
            referenced_guids.setdefault(key + "guid", []).append(ref.guid)
            referenced_names.setdefault(key, []).append(ref.name)
    # Query all children entities
    appsearch_children = list(
        update_children_breadcrumb(entity_details, elastic, index_name, breadcrumbs),
    )
    # Merge related entities and children entities
    for child in appsearch_children:
        # Is child related to the main entity
        if child.guid in related_dict:
            related_doc = related_dict[child.guid]
            related_doc.breadcrumbname = child.breadcrumbname
            related_doc.breadcrumbguid = child.breadcrumbguid
            related_doc.breadcrumbtype = child.breadcrumbtype
            # Add immediate parent
            related_doc.parentguid = child.breadcrumbguid[-1] if child.breadcrumbguid else None

    # Merge related entities and all children entities
    related = list(related_dict.values()) + [child for child in appsearch_children if child.guid not in related_dict]

    return related, referenced_guids | referenced_names


def default_create_handler(
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
) -> list[AppSearchDocument]:
    """
    Create `AppSearchDocument` instance and update existing ones using the provided entity details.

    Parameters
    ----------
    entity_details : Entity
        The entity details to extract the necessary attributes from.
    elastic : Elasticsearch
        The Elasticsearch client for database interaction.
    index_name : str
        The name of the index in Elasticsearch to query.

    Returns
    -------
    AppSearchDocument
        List of AppSearchDocument instances representing the created entity and related entities.
    """
    # Set attributes of the main entity
    breadcrumbs: dict = get_breadcrumbs_of_entity(entity_details, elastic, index_name)
    qualified_name = getattr(entity_details.attributes, "qualified_name", entity_details.guid)
    name = getattr(entity_details.attributes, "name", qualified_name)
    # Update children and related entities of the main entity
    docs, references = update_existing_documents(entity_details, elastic, index_name, breadcrumbs)
    # Immediate parent if exists
    parentguid = breadcrumbs["breadcrumbguid"][-1] if breadcrumbs["breadcrumbguid"] else None
    # Merge
    docs.insert(
        0,
        AppSearchDocument(
            id=entity_details.guid,
            guid=entity_details.guid,
            typename=entity_details.type_name,
            name=name,
            referenceablequalifiedname=qualified_name,
            breadcrumbname=breadcrumbs["breadcrumbname"],
            breadcrumbguid=breadcrumbs["breadcrumbguid"],
            breadcrumbtype=breadcrumbs["breadcrumbtype"],
            supertypenames=[entity_details.type_name],
            parentguid=parentguid,
            **references,
        ),
    )

    return docs


def create_person_handler(
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
) -> list[AppSearchDocument]:
    """
    Create an `AppSearchDocument` instance for a person entity using the provided entity details.

    Parameters
    ----------
    entity_details : Entity
        The person entity details to extract the necessary attributes from.
    elastic : Elasticsearch
        The Elasticsearch client for database interaction.
    index_name : str
        The name of the index in Elasticsearch to query.

    Returns
    -------
    AppSearchDocument
        The created AppSearchDocument instance.
    """
    result = default_create_handler(entity_details, elastic, index_name)

    attributes: dict[str, str] = cast(dict, entity_details.attributes.unmapped_attributes)

    if "email" in attributes:
        result[0].email = attributes["email"]

    return result


ENTITY_CREATED_HANDLERS = {"m4i_person": create_person_handler}


def handle_entity_created(
    message: EntityMessage,
    elastic: Elasticsearch,
    index_name: str,
) -> list[AppSearchDocument]:
    """
    Process the entity creation message and create `AppSearchDocument`s accordingly.

    EntityMessage should contain an entity from data_dictionary, so that
    the referred entities will not be an empty list.

    Parameters
    ----------
    message : EntityMessage
        The EntityMessage instance containing the entity creation details.
    elastic : Elasticsearch
        The Elasticsearch client for database interaction.
    index_name : str
        The name of the index in Elasticsearch to query.

    Returns
    -------
    list[AppSearchDocument]
        A list containing the created and updated `AppSearchDocument`s.
        The first element is for the created entity.
        The rest is for its references.

    Raises
    ------
    EntityDataNotProvidedError
        If the entity details are not provided in the message.
    """
    entity_details = message.new_value

    if entity_details is None:
        raise EntityDataNotProvidedError(message.guid)
    # END IF

    create_handler = ENTITY_CREATED_HANDLERS.get(entity_details.type_name, default_create_handler)

    return create_handler(entity_details, elastic, index_name)
