from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json
from m4i_atlas_core import AtlasChangeMessage, Entity

from flink_jobs.elastic_client import ElasticSearchEntity


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class KafkaNotification(DataClassJsonMixin):
    """Describes the structure of a Kafka notification from Apache Atlas."""

    msg_creation_time: int
    event_time: int
    atlas_entity_audit: dict
    kafka_notification: AtlasChangeMessage
    atlas_entity: Entity | None = None
    previous_version: ElasticSearchEntity | None = None
