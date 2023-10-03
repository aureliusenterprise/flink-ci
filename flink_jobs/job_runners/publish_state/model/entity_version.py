from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json
from m4i_atlas_core import Entity


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class EntityVersion(DataClassJsonMixin):
    body: Entity
    doc_id: str
    event_time: int
    msg_creation_time: int
