from typing import cast

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


def default_create_handler(entity_details: Entity) -> AppSearchDocument:
    """
    Create an AppSearchDocument instance using the provided entity details.

    Parameters
    ----------
    entity_details : Entity
        The entity details to extract the necessary attributes from.

    Returns
    -------
    AppSearchDocument
        The created AppSearchDocument instance.
    """
    attributes: dict[str, str] = cast(dict, entity_details.attributes.unmapped_attributes)

    qualified_name = attributes["qualifiedName"]
    name = attributes.get("name", qualified_name)

    return AppSearchDocument(
        id=entity_details.guid,
        guid=entity_details.guid,
        typename=entity_details.type_name,
        name=name,
        referenceablequalifiedname=qualified_name,
    )


def create_person_handler(entity_details: Entity) -> AppSearchDocument:
    """
    Create an AppSearchDocument instance for a person entity using the provided entity details.

    Parameters
    ----------
    entity_details : Entity
        The person entity details to extract the necessary attributes from.

    Returns
    -------
    AppSearchDocument
        The created AppSearchDocument instance.
    """
    result = default_create_handler(entity_details)

    attributes: dict[str, str] = cast(dict, entity_details.attributes.unmapped_attributes)

    if "email" in attributes:
        result.email = attributes["email"]

    return result


ENTITY_CREATED_HANDLERS = {"m4i_person": create_person_handler}


def handle_entity_created(
    message: EntityMessage,
) -> list[AppSearchDocument]:
    """
    Process the entity creation message and create an AppSearchDocument instance accordingly.

    Parameters
    ----------
    message : EntityMessage
        The EntityMessage instance containing the entity creation details.

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

    return [create_handler(entity_details)]
