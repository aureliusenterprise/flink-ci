from collections.abc import Generator
from typing import cast

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from m4i_atlas_core import Entity, M4IAttributes

from flink_tasks import AppSearchDocument, EntityMessage, SynchronizeAppSearchError

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
) -> tuple[list[str], list[str], list[str]]:
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
    tuple[list[str], list[str], list[str]]
        A tuple containing lists of updated breadcrumb details - name, GUID, and type.
    """
    if input_entity is None:
        return [], [], []

    # Get the parents of the entity
    parents = [x.guid for x in input_entity.get_parents()]

    if not parents:
        return [], [], []

    # Look up breadcrumbs of first parent
    breadcrumb_name, breadcrumb_guid, breadcrumb_type = [], [], []
    query = {"query": {"terms": {"breadcrumb_guid": parents[:1]}}}

    for document in get_documents(query, elastic, index_name):
        breadcrumb_name += [*document.breadcrumbname, document.name]
        breadcrumb_guid += [*document.breadcrumbguid, document.guid]
        breadcrumb_type += [*document.breadcrumbtype, document.typename]

    return breadcrumb_name, breadcrumb_guid, breadcrumb_type


def create_derived_relations(
    related_guids: list[str],
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
) -> Generator[AppSearchDocument, None, None]:
    """
    Update existing `AppSearchDocument`s that represent entities related to the provided entity.

    Parameters
    ----------
    related_guids : list[str]
        List of GUIDs representing the related entities.
    entity_details: Entity
        Details of the main entity for which derived relations are created.
    elastic: Elasticsearch
        Elasticsearch client for querying documents.
    index_name: str
        Name of the Elasticsearch index containing the relevant documents.

    Returns
    -------
    Generator[AppSearchDocument, None, None]
        A generator yielding AppSearchDocument objects representing the derived relations.
    """
    if not related_guids:
        return

    query = {"query": {"terms": {"guid": related_guids}}}

    for document in get_documents(query, elastic, index_name):
        for key in RELATIONSHIP_MAP[entity_details.type_name]:
            # The query guarantees that the relationship attributes are present in the document.
            # No need for try/except block to handle a potential KeyError.
            guids: list[str] = getattr(document, key + "guid")
            names: list[str] = getattr(document, key)
            # Update guid and name with a fallback value to qualifiedName
            if hasattr(entity_details.attributes, "name"):
                name = entity_details.attributes.name  # type: ignore
            else:
                attr: dict[str, str] = cast(dict, entity_details.attributes.unmapped_attributes)
                name = attr["qualifiedName"]
            guids.append(entity_details.guid)
            names.append(name)
        yield document


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
    qualified_name = getattr(entity_details.attributes, "qualified_name", entity_details.guid)
    name = getattr(entity_details.attributes, "name", qualified_name)

    # Get breadcrumbs
    b_names, b_guids, b_types = get_breadcrumbs_of_entity(entity_details, elastic, index_name)

    # Get referenced entities
    references, referred_guids = {}, []

    for ref in entity_details.get_referred_entities():
        keys = RELATIONSHIP_MAP[ref.type_name]
        for key in keys:
            # Referenced entity's guid
            references.setdefault(key + "guid", []).append(ref.guid)
            referred_guids.append(ref.guid)
            # Referenced entity's name
            unique = cast(M4IAttributes, ref.unique_attributes)
            unmapped: dict[str, str] = cast(dict, unique.unmapped_attributes)
            # Set qualified_name as fallback value
            references.setdefault(key, []).append(unmapped.get("name", unique.qualified_name))

    related = list(create_derived_relations(referred_guids, entity_details, elastic, index_name))

    related.insert(
        0,
        AppSearchDocument(
            id=entity_details.guid,
            guid=entity_details.guid,
            typename=entity_details.type_name,
            name=name,
            referenceablequalifiedname=qualified_name,
            breadcrumbname=b_names,
            breadcrumbguid=b_guids,
            breadcrumbtype=b_types,
            **references,
        ),
    )

    return related


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
