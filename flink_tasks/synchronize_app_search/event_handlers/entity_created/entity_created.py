from collections.abc import Generator
from typing import cast

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from m4i_atlas_core import Entity

from flink_tasks import AppSearchDocument, EntityMessage


class EntityDataNotProvidedError(Exception):
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
    Update entity breadcrumbs based on the provided input_entity.

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

    # Get parents of entity
    parent_guids = [x.guid for x in input_entity.get_parents()]
    breadcrumb_name, breadcrumb_guid, breadcrumb_type = [], [], []
    # Look up breadcrumbs of parents
    query = {"query": {"terms": {"breadcrumb_guid": parent_guids}}}

    for document in get_documents(query, elastic, index_name):
        breadcrumb_name += [*document.breadcrumbname, document.name]
        breadcrumb_guid += [*document.breadcrumbguid, document.guid]
        breadcrumb_type += [*document.breadcrumbtype, document.typename]

    return breadcrumb_name, breadcrumb_guid, breadcrumb_type


def default_create_handler(
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
) -> AppSearchDocument:
    """
    Create an AppSearchDocument instance using the provided entity details.

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
        The created AppSearchDocument instance.
    """
    attributes: dict[str, str] = cast(dict, entity_details.attributes.unmapped_attributes)

    qualified_name = attributes["qualifiedName"]
    name = attributes.get("name", qualified_name)

    # Get breadcrumbs
    b_names, b_guids, b_types = get_breadcrumbs_of_entity(entity_details, elastic, index_name)

    return AppSearchDocument(
        id=entity_details.guid,
        guid=entity_details.guid,
        typename=entity_details.type_name,
        name=name,
        referenceablequalifiedname=qualified_name,
        breadcrumbname=b_names,
        breadcrumbguid=b_guids,
        breadcrumbtype=b_types,
    )


def create_person_handler(
    entity_details: Entity,
    elastic: Elasticsearch,
    index_name: str,
) -> AppSearchDocument:
    """
    Create an AppSearchDocument instance for a person entity using the provided entity details.

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
        result.email = attributes["email"]

    return result


ENTITY_CREATED_HANDLERS = {"m4i_person": create_person_handler}


def handle_entity_created(
    message: EntityMessage,
    elastic: Elasticsearch,
    index_name: str,
) -> list[AppSearchDocument]:
    """
    Process the entity creation message and create an AppSearchDocument instance accordingly.

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
        A list containing the created AppSearchDocument instance.

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

    return [create_handler(entity_details, elastic, index_name)]
