from unittest.mock import Mock, patch

import pytest
from m4i_atlas_core import (
    Attributes,
    BusinessDataEntity,
    BusinessDataEntityAttributes,
    Entity,
    EntityAuditAction,
    M4IAttributes,
    ObjectId,
)

from flink_tasks import AppSearchDocument, EntityMessage, EntityMessageType

from .entity_created import (
    EntityDataNotProvidedError,
    handle_entity_created,
    update_children_breadcrumb,
)


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

    with patch(
        "flink_tasks.synchronize_app_search.event_handlers.entity_created.entity_created.get_documents",
        return_value=[],
    ):
        result = handle_entity_created(entity_message, Mock(), "test_index")

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

    with patch(
        "flink_tasks.synchronize_app_search.event_handlers.entity_created.entity_created.get_documents",
        return_value=[],
    ):
        result = handle_entity_created(entity_message, Mock(), "test_index")

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
    with patch(
        "flink_tasks.synchronize_app_search.event_handlers.entity_created.entity_created.get_documents",
        return_value=[],
    ):
        result = handle_entity_created(entity_message, Mock(), "test_index")

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
        handle_entity_created(entity_message, Mock(), "test_index")


def test__handle_entity_created_with_breadcrumbs() -> None:
    """
    Verify that handle_entity_created also creates breadcrumbs.

    Asserts
    - breadcrumbs are added
    """
    business_data_entity = BusinessDataEntity(
        guid="1111",
        type_name="m4i_data_entity",
        attributes=BusinessDataEntityAttributes.from_dict({
            "qualifiedName": "test-data-entity",
            "name": "test entity",
            "unmapped_attributes": {"qualifiedName": "test-data-entity"},
            }),
    )

    parent_ref = ObjectId(
        guid="2222",
        type_name="m4i_data_domain",
        unique_attributes=M4IAttributes(qualified_name="test object"),
    )

    business_data_entity.attributes.data_domain = [parent_ref]

    entity_message = EntityMessage(
        type_name="m4i_data_entity",
        guid="1234",
        original_event_type=EntityAuditAction.ENTITY_CREATE,
        event_type=EntityMessageType.ENTITY_CREATED,
        new_value=business_data_entity,
    )

    parent_document = AppSearchDocument(
        guid="2345",
        typename="m4i_data_domain",
        name="Domain Name",
        referenceablequalifiedname="entity_name",
        breadcrumbguid=["5678"],
        breadcrumbname=["Parent Data Domain Name"],
        breadcrumbtype=["m4i_data_domain"],
    )

    with patch(
        "flink_tasks.synchronize_app_search.event_handlers.entity_created.entity_created.get_documents",
        return_value=[parent_document],
    ):
        result = handle_entity_created(entity_message, Mock(), "test_index")

        assert len(result) == 2

        document = result[0]

        assert document.breadcrumbname == ["Parent Data Domain Name", "Domain Name"]
        assert document.breadcrumbguid == ["5678", "2345"]
        assert document.breadcrumbtype == ["m4i_data_domain", "m4i_data_domain"]

def test__handle_entity_created_add_relations() -> None:
    """Verify that the created entity's relations are also added."""
    business_data_entity = BusinessDataEntity(
        guid="1111",
        type_name="m4i_data_entity",
        attributes=BusinessDataEntityAttributes.from_dict({
            "qualifiedName": "test-data-entity",
            "name": "test entity",
            "unmapped_attributes":{"qualifiedName": "test-data-entity"},
            }),
    )

    business_data_entity.attributes.data_domain = [ObjectId(
        guid="2222",
        type_name="m4i_data_domain",
        unique_attributes=M4IAttributes(
            qualified_name="test object",
            unmapped_attributes={"name": "My Data Domain"},
        ),
    )]

    entity_message = EntityMessage(
        type_name="m4i_data_entity",
        guid="1234",
        original_event_type=EntityAuditAction.ENTITY_CREATE,
        event_type=EntityMessageType.ENTITY_CREATED,
        new_value=business_data_entity,
    )

    related_document = AppSearchDocument(
        guid="2345",
        typename="m4i_data_domain",
        name="Domain Name",
        referenceablequalifiedname="entity_name",
        breadcrumbguid=["5678"],
        breadcrumbname=["Parent Data Domain Name"],
        breadcrumbtype=["m4i_data_domain"],
    )

    with patch(
        "flink_tasks.synchronize_app_search.event_handlers.entity_created.entity_created.get_documents",
        return_value=[related_document],
    ):
        result = handle_entity_created(entity_message, Mock(), "test_index")

        document = result[0]

        assert document.deriveddatadomainguid == ["2222"]
        assert document.deriveddatadomain == ["My Data Domain"]

        related_document = result[1]

        assert related_document.deriveddataentityguid == ["1111"]
        assert related_document.deriveddataentity == ["test entity"]


def test__handle_entity_created_multiple_relations() -> None:
    """Verify that multiple relations are created."""
    data_entity = BusinessDataEntity(
        guid="1111",
        type_name="m4i_data_entity",
        attributes=BusinessDataEntityAttributes.from_dict({
            "qualifiedName": "1111-data-entity",
            "name": "Test Entity",
            "unmapped_attributes":{"qualifiedName": "1111-data-entity"},
            }),
    )

    data_entity.attributes.data_domain = [ObjectId(
        guid="2222",
        type_name="m4i_data_domain",
        unique_attributes=M4IAttributes(
            qualified_name="2222-domain",
            unmapped_attributes={"name": "Test Data Domain"},
        ),
    )]

    data_entity.attributes.attributes = [ObjectId(
        guid="3333",
        type_name="m4i_data_attribute",
        unique_attributes=M4IAttributes(
            qualified_name="3333-attribute",
            unmapped_attributes={"name": "Test Data Attribute"},
        ),
    )]

    message = EntityMessage(
        type_name="m4i_data_entity",
        guid="0000",
        original_event_type=EntityAuditAction.ENTITY_CREATE,
        event_type=EntityMessageType.ENTITY_CREATED,
        new_value=data_entity,
    )

    related_1 = AppSearchDocument(
        guid="2222",
        typename="m4i_data_domain",
        name="Domain Name",
        referenceablequalifiedname="data_domain_name",
    )

    related_2 = AppSearchDocument(
        guid="3333",
        typename="m4i_data_attribute",
        name="Test Data Attribute",
        referenceablequalifiedname="data_attribute_name",
    )

    with patch(
            "flink_tasks.synchronize_app_search.event_handlers.entity_created.entity_created.get_documents",
            return_value=[related_1, related_2],
    ):
        result = handle_entity_created(message, Mock(), "test_index")

        # Assert created entity's relations
        assert result[0].deriveddatadomainguid == ["2222"]
        assert result[0].deriveddatadomain == ["Test Data Domain"]
        assert result[0].deriveddataattributeguid == ["3333"]
        assert result[0].deriveddataattribute == ["Test Data Attribute"]
        # Assert relating back to the data entity
        assert result[1].deriveddataentityguid == ["1111"]
        assert result[1].deriveddataentity == ["Test Entity"]
        assert result[2].deriveddataentityguid == ["1111"]
        assert result[2].deriveddataentity == ["Test Entity"]


def test__handle_entity_created_children_breadcrumb() -> None:
    """Verify that the breadcrumb of children are updated."""
    data_entity = BusinessDataEntity(
        guid="1111",
        type_name="m4i_data_entity",
        attributes=BusinessDataEntityAttributes.from_dict({
            "qualifiedName": "1111-data-entity",
            "name": "Test Entity",
            "unmapped_attributes": {"qualifiedName": "1111-data-entity", "name": "Test Entity"},
        }),
    )

    data_entity.attributes.attributes = [ObjectId(
        guid="3333",
        type_name="m4i_data_attribute",
        unique_attributes=M4IAttributes(
            qualified_name="3333-attribute",
            unmapped_attributes={"name": "Test Data Attribute"},
        ),
    )]

    child_relation = AppSearchDocument(
        guid="3333",
        typename="m4i_data_attribute",
        name="Test Data Attribute",
        referenceablequalifiedname="data_attribute_name",
    )

    with patch(
            "flink_tasks.synchronize_app_search.event_handlers.entity_created.entity_created.get_documents",
            return_value=[child_relation],
    ):
        result = list(update_children_breadcrumb(
            data_entity, Mock(), "test_index", ["Parent name"], ["Parent id"], ["Parent type"],
        ))

        # Assert children received the main entity and its parent entities breadcrumb information
        assert result[0].breadcrumbname == ["Parent name", "Test Entity"]
        assert result[0].breadcrumbguid == ["Parent id", "1111"]
        assert result[0].breadcrumbtype == ["Parent type", "m4i_data_entity"]
