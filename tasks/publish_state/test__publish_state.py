from unittest import mock

import pytest
from m4i_atlas_core import (
    AtlasChangeMessage,
    AtlasChangeMessageBody,
    AtlasChangeMessageVersion,
    Entity,
    EntityAuditAction,
    EntityNotificationType,
)
from marshmallow import ValidationError
from pyflink.datastream import StreamExecutionEnvironment

from .model import KafkaNotification, ValidatedInput
from .publish_state import PublishState


@pytest.fixture()
def environment() -> StreamExecutionEnvironment:
    """
    Pytest fixture to provide a StreamExecutionEnvironment for testing.

    Returns
    -------
    StreamExecutionEnvironment
        An instance of Flink's StreamExecutionEnvironment.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    return env

@pytest.fixture()
def event() -> KafkaNotification:
    """
    Pytest fixture to provide a sample KafkaNotification event for testing.

    Returns
    -------
    KafkaNotification
        A sample KafkaNotification object.
    """
    return KafkaNotification(
        msg_creation_time=1,
        event_time=1,
        atlas_entity_audit={},
        kafka_notification=AtlasChangeMessage(
            version=AtlasChangeMessageVersion(
                version="1",
                version_parts=[],
            ),
            msg_compression_kind="",
            msg_split_idx=1,
            msg_split_count=1,
            msg_source_ip="localhost",
            msg_created_by="test",
            msg_creation_time=1,
            message=AtlasChangeMessageBody(
                event_time=1,
                operation_type=EntityAuditAction.ENTITY_CREATE,
                type=EntityNotificationType.ENTITY_NOTIFICATION_V1,
                entity=Entity(guid="1234"),
                relationship=None,
            ),
        ),
    )

def test__publish_state_validate_input(
    environment: StreamExecutionEnvironment,
    event: KafkaNotification,
) -> None:
    """
    Test the validation stage of the PublishState.

    This test checks if the input `KafkaNotification` event is correctly validated and transformed
    into a `ValidatedInput` object.
    """
    data_stream = environment.from_collection([event.to_json()])

    publish_state = PublishState(data_stream, mock.Mock())

    expected = [
        ValidatedInput(
            entity=Entity(guid="1234"),
            event_time=1,
            msg_creation_time=1,
        ),
    ]

    output = list(publish_state.input_validation.main.execute_and_collect())

    assert output == expected

def test__publish_state_validate_input_with_invalid_input(
    environment: StreamExecutionEnvironment,
) -> None:
    """
    Test the validation stage of the `PublishState` flow with invalid input.

    This test checks if the input `KafkaNotification`, which is deliberately made invalid,
    is correctly captured as a validation error and emitted on the `errors` output.
    """
    invalid_input = '{"msgCreationTime": 1, "eventTime": 1, "atlasEntityAudit": {}}'
    data_stream = environment.from_collection([invalid_input])

    publish_state = PublishState(data_stream, mock.Mock())

    output = list(publish_state.errors.execute_and_collect())

    assert len(output) == 1

    validation_error = output[0]

    assert isinstance(validation_error, ValidationError)

    expected_messages = {
        "kafkaNotification": ["Missing data for required field."],
    }

    assert validation_error.messages == expected_messages
