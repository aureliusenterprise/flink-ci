from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json
from m4i_atlas_core import AtlasChangeMessage, Entity

from flink_tasks.elastic_client import ElasticSearchEntity


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class KafkaNotification(DataClassJsonMixin):
    """
    Describes the structure of a Kafka notification from Apache Atlas.

    The `KafkaNotification` captures the details of the changes related to
    entities in Apache Atlas and tracks their previous versions if available.

    Attributes
    ----------
    msg_creation_time : int
        The timestamp (epoch) indicating when the notification message was created.

    event_time : int
        The timestamp (epoch) indicating when the event triggering the notification occurred.

    atlas_entity_audit : dict
        A dictionary containing the audit information related to the change in Apache Atlas.

    kafka_notification : AtlasChangeMessage
        The change message from Apache Atlas, detailing the modifications made.

    atlas_entity : Entity, optional
        The entity from Apache Atlas associated with the notification. Defaults to None.

    previous_version : ElasticSearchEntity, optional
        The previous version of the entity from Elasticsearch, before the change occurred.
        Defaults to None.
    """

    msg_creation_time: int #  msg_creation_time = kafka_notification.msg_creation_time
    event_time: int  # event_time = kafka_notification.event_time
    atlas_entity_audit: dict
    kafka_notification: AtlasChangeMessage
    atlas_entity: Entity | None = None  # atlas_entity = kafka_notification.atlas_entity
    previous_version: ElasticSearchEntity | None = None  # this is wrong
    # why are the supertypes not in anymore?
