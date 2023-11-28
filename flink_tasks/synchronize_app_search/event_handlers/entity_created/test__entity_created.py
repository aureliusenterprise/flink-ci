import pytest
from m4i_atlas_core import Attributes, Entity, EntityAuditAction

from flink_tasks import EntityMessage, EntityMessageType

from .entity_created import EntityDataNotProvidedError, handle_entity_created


def test__default_create_handler_with_complete_details() -> None:
    """
    Verify that `handle_entity_created` correctly processes an entity.

    Asserts:
    - The function produces one AppSearchDocument.
    - The AppSearchDocument's attributes correctly reflect the entity's details.
    """
    entity_message = EntityMessage(
        type_name="m4i_data_domain",
        guid="1234",
        original_event_type=EntityAuditAction.ENTITY_CREATE,
        event_type=EntityMessageType.ENTITY_CREATED,
        new_value=Entity(
            guid="1234",
            type_name="m4i_data_domain",
            attributes=Attributes.from_dict({"qualifiedName": "1234-test", "name": "test"}),
        ),
    )

    result = handle_entity_created(entity_message)

    assert len(result) == 1

    document = result[0]

    assert document.id == "1234"
    assert document.guid == "1234"
    assert document.typename == "m4i_data_domain"
    assert document.name == "test"
    assert document.referenceablequalifiedname == "1234-test"


def test__default_create_handler_without_name() -> None:
    """
    Verify that `handle_entity_created` uses `qualifiedName` as `name` by default.

    Asserts:
    - The function produces one AppSearchDocument.
    - The `name` attribute of the AppSearchDocument matches the `qualifiedName` of the entity.
    """
    entity_message = EntityMessage(
        type_name="m4i_data_domain",
        guid="1234",
        original_event_type=EntityAuditAction.ENTITY_CREATE,
        event_type=EntityMessageType.ENTITY_CREATED,
        new_value=Entity(
            guid="1234",
            type_name="m4i_data_domain",
            attributes=Attributes.from_dict({"qualifiedName": "1234-test"}),
        ),
    )

    result = handle_entity_created(entity_message)

    assert len(result) == 1

    document = result[0]

    assert document.name == "1234-test"


def test__create_person_handler_with_email() -> None:
    """
    Verify that `handle_entity_created` correctly processes a person entity.

    Asserts:
    - The function produces one AppSearchDocument.
    - The AppSearchDocument includes the `email` attribute, matching the entity's email.
    """
    entity_message = EntityMessage(
        type_name="m4i_person",
        guid="1234",
        original_event_type=EntityAuditAction.ENTITY_CREATE,
        event_type=EntityMessageType.ENTITY_CREATED,
        new_value=Entity(
            guid="1234",
            type_name="m4i_person",
            attributes=Attributes.from_dict(
                {"qualifiedName": "1234-test", "name": "test", "email": "john.doe@example.com"},
            ),
        ),
    )

    result = handle_entity_created(entity_message)

    assert len(result) == 1

    document = result[0]

    assert document.id == "1234"
    assert document.guid == "1234"
    assert document.typename == "m4i_person"
    assert document.name == "test"
    assert document.referenceablequalifiedname == "1234-test"
    assert document.email == "john.doe@example.com"


def test__handle_entity_created_missing_entity_data() -> None:
    """
    Verify that `handle_entity_created` raises `EntityDataNotProvidedError` when no entity is given.

    Asserts:
    - `EntityDataNotProvidedError` is raised when the entity message lacks entity details.
    """
    entity_message = EntityMessage(
        type_name="m4i_data_domain",
        guid="1234",
        original_event_type=EntityAuditAction.ENTITY_CREATE,
        event_type=EntityMessageType.ENTITY_CREATED,
    )

    with pytest.raises(EntityDataNotProvidedError):
        handle_entity_created(entity_message)
