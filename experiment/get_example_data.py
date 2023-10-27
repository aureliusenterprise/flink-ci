from m4i_atlas_core import (
    AtlasChangeMessage,
    AtlasChangeMessageBody,
    AtlasChangeMessageVersion,
    Entity,
    EntityAuditAction,
    EntityNotificationType,
)

from flink_jobs.publish_state.model.kafka_notification import KafkaNotification

dd = KafkaNotification(
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
print(dd.to_json())  # noqa: T201
