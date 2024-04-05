from unittest.mock import Mock, patch

from m4i_atlas_core import (
    BusinessDataDomain,
    BusinessDataDomainAttributes,
    EntityAuditAction,
    M4IAttributes,
    ObjectId,
)

from flink_tasks import AppSearchDocument, EntityMessage, EntityMessageType
from flink_tasks.operations.synchronize_app_search.transaction import Transaction

from .relationship_audit import handle_relationship_audit


def test__handle_relationship_audit_inserted_relationship() -> None:
    """
    Test that the `handle_relationship_audit` function correctly handles added relationships.

    The function should update the documents with the new relationships and return the updated
    documents.

    Asserts
    -------
    - The updated documents contain the new relationships
    - The updated documents contain the correct breadcrumb information
    """
    message = EntityMessage(
        type_name="m4i_data_domain",
        guid="2345",
        original_event_type=EntityAuditAction.ENTITY_UPDATE,
        event_type=EntityMessageType.ENTITY_RELATIONSHIP_AUDIT,
        new_value=BusinessDataDomain(
            guid="2345",
            type_name="m4i_data_domain",
            attributes=BusinessDataDomainAttributes(
                qualified_name="data_domain",
                name="Data Domain",
                data_entity=[
                    ObjectId(
                        type_name="m4i_data_entity",
                        guid="1234",
                        unique_attributes=M4IAttributes.from_dict({"qualifiedName": "Entity Name"}),
                    ),
                ],
            ),
        ),
        inserted_relationships={
            "data_entity": [
                ObjectId(
                    type_name="m4i_data_entity",
                    guid="1234",
                    unique_attributes=M4IAttributes.from_dict({"qualifiedName": "Entity Name"}),
                ),
            ],
        },
    )

    current_document = AppSearchDocument(
        guid="2345",
        typename="m4i_data_domain",
        name="Domain Name",
        referenceablequalifiedname="domain_name",
    )

    children = [
        AppSearchDocument(
            guid="1234",
            typename="m4i_data_entity",
            name="Data Entity",
            referenceablequalifiedname="data_entity",
            deriveddataentity=["Data Entity"],
            deriveddataentityguid=["3456"],
        ),
    ]

    descendants = [
        AppSearchDocument(
            guid="3456",
            typename="m4i_data_entity",
            name="Data Entity",
            referenceablequalifiedname="data_entity",
            deriveddataentity=["Data Entity"],
            deriveddataentityguid=["1234"],
            deriveddataattribute=["Attribute"],
            deriveddataattributeguid=["4567"],
            breadcrumbguid=["1234"],
            breadcrumbname=["Data Entity"],
            breadcrumbtype=["m4i_data_entity"],
            parentguid="1234",
        ),
        AppSearchDocument(
            guid="4567",
            typename="m4i_data_attribute",
            name="Attribute",
            referenceablequalifiedname="attribute",
            deriveddataentity=["Data Entity"],
            deriveddataentityguid=["3456"],
            breadcrumbguid=["1234", "3456"],
            breadcrumbname=["Data Entity", "Data Entity"],
            breadcrumbtype=["m4i_data_entity", "m4i_data_entity"],
            parentguid="3456",
        ),
    ]

    with (
        patch(
            __package__ + ".relationship_audit.find_children",  # type: ignore
            return_value=children,
        ),
        patch(
            __package__ + ".relationship_audit.find_descendants",  # type: ignore
            return_value=descendants,
        ),
        patch.object(Transaction, "find_one", Mock(return_value=current_document)),
    ):
        updated_documents = dict(handle_relationship_audit(message, Transaction(Mock(), "test_index")))

        assert len(updated_documents) == 4

        updated_domain = updated_documents["2345"]
        assert updated_domain is not None
        assert updated_domain.deriveddataentity == ["Data Entity"]
        assert updated_domain.deriveddataentityguid == ["1234"]

        updated_entity = updated_documents["1234"]
        assert updated_entity is not None
        assert updated_entity.deriveddatadomain == ["Domain Name"]
        assert updated_entity.deriveddatadomainguid == ["2345"]
        assert updated_entity.breadcrumbguid == ["2345"]
        assert updated_entity.breadcrumbname == ["Domain Name"]
        assert updated_entity.breadcrumbtype == ["m4i_data_domain"]
        assert updated_entity.parentguid == "2345"

        updated_subentity = updated_documents["3456"]
        assert updated_subentity is not None
        assert updated_subentity.breadcrumbguid == ["2345", "1234"]
        assert updated_subentity.breadcrumbname == ["Domain Name", "Data Entity"]
        assert updated_subentity.breadcrumbtype == ["m4i_data_domain", "m4i_data_entity"]
        assert updated_subentity.parentguid == "1234"

        updated_attribute = updated_documents["4567"]
        assert updated_attribute is not None
        assert updated_attribute.breadcrumbguid == ["2345", "1234", "3456"]
        assert updated_attribute.breadcrumbname == ["Domain Name", "Data Entity", "Data Entity"]
        assert updated_attribute.breadcrumbtype == ["m4i_data_domain", "m4i_data_entity", "m4i_data_entity"]
        assert updated_attribute.parentguid == "3456"


def test__handle_relationship_audit_deleted_relationship() -> None:
    """
    Test that the `handle_relationship_audit` function correctly handles deleted relationships.

    The function should update the documents with the removed relationships and return the updated
    documents.

    Asserts
    -------
    - The updated documents do not contain the removed relationships
    - The updated documents contain the correct breadcrumb information
    """
    message = EntityMessage(
        type_name="m4i_data_domain",
        guid="2345",
        original_event_type=EntityAuditAction.ENTITY_UPDATE,
        event_type=EntityMessageType.ENTITY_RELATIONSHIP_AUDIT,
        old_value=BusinessDataDomain(
            guid="2345",
            type_name="m4i_data_domain",
            attributes=BusinessDataDomainAttributes(
                qualified_name="data_domain",
                name="Data Domain",
                data_entity=[
                    ObjectId(
                        type_name="m4i_data_entity",
                        guid="1234",
                        unique_attributes=M4IAttributes.from_dict({"qualifiedName": "Entity Name"}),
                    ),
                ],
            ),
        ),
        deleted_relationships={
            "data_entity": [
                ObjectId(
                    type_name="m4i_data_entity",
                    guid="1234",
                    unique_attributes=M4IAttributes.from_dict({"qualifiedName": "Entity Name"}),
                ),
            ],
        },
    )

    current_document = AppSearchDocument(
        guid="2345",
        typename="m4i_data_domain",
        name="Domain Name",
        referenceablequalifiedname="domain_name",
    )

    children = [
        AppSearchDocument(
            guid="1234",
            typename="m4i_data_entity",
            name="Data Entity",
            referenceablequalifiedname="data_entity",
            deriveddataentity=["Data Entity"],
            deriveddataentityguid=["3456"],
            breadcrumbguid=["2345"],
            breadcrumbname=["Domain Name"],
            breadcrumbtype=["m4i_data_domain"],
        ),
    ]

    descendants = [
        AppSearchDocument(
            guid="3456",
            typename="m4i_data_entity",
            name="Data Entity",
            referenceablequalifiedname="data_entity",
            deriveddataentity=["Data Entity"],
            deriveddataentityguid=["1234"],
            deriveddataattribute=["Attribute"],
            deriveddataattributeguid=["4567"],
            breadcrumbguid=["2345", "1234"],
            breadcrumbname=["Domain Name", "Data Entity"],
            breadcrumbtype=["m4i_data_domain", "m4i_data_entity"],
            parentguid="1234",
        ),
        AppSearchDocument(
            guid="4567",
            typename="m4i_data_attribute",
            name="Attribute",
            referenceablequalifiedname="attribute",
            deriveddataentity=["Data Entity"],
            deriveddataentityguid=["3456"],
            breadcrumbguid=["2345", "1234", "3456"],
            breadcrumbname=["Domain Name", "Data Entity", "Data Entity"],
            breadcrumbtype=["m4i_data_domain", "m4i_data_entity", "m4i_data_entity"],
            parentguid="3456",
        ),
    ]

    with (
        patch(
            __package__ + ".relationship_audit.find_children",  # type: ignore
            return_value=children,
        ),
        patch(
            __package__ + ".relationship_audit.find_descendants",  # type: ignore
            return_value=descendants,
        ),
        patch.object(Transaction, "find_one", Mock(return_value=current_document)),
    ):
        updated_documents = dict(handle_relationship_audit(message, Transaction(Mock(), "test_index")))

        assert len(updated_documents) == 4

        updated_domain = updated_documents["2345"]
        assert updated_domain is not None
        assert updated_domain.deriveddataentity == []
        assert updated_domain.deriveddataentityguid == []

        updated_entity = updated_documents["1234"]
        assert updated_entity is not None
        assert updated_entity.deriveddatadomain == []
        assert updated_entity.deriveddatadomainguid == []
        assert updated_entity.breadcrumbguid == []
        assert updated_entity.breadcrumbname == []
        assert updated_entity.breadcrumbtype == []
        assert updated_entity.parentguid is None

        updated_subentity = updated_documents["3456"]
        assert updated_subentity is not None
        assert updated_subentity.breadcrumbguid == ["1234"]
        assert updated_subentity.breadcrumbname == ["Data Entity"]
        assert updated_subentity.breadcrumbtype == ["m4i_data_entity"]
        assert updated_subentity.parentguid == "1234"

        updated_attribute = updated_documents["4567"]
        assert updated_attribute is not None
        assert updated_attribute.breadcrumbguid == ["1234", "3456"]
        assert updated_attribute.breadcrumbname == ["Data Entity", "Data Entity"]
        assert updated_attribute.breadcrumbtype == ["m4i_data_entity", "m4i_data_entity"]
        assert updated_attribute.parentguid == "3456"

