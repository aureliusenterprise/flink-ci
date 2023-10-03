import time
from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json


@dataclass_json(letter_case=LetterCase.CAMEL)  # type: ignore
@dataclass
class DeadLetterBoxMesage(DataClassJsonMixin):
    """Describes the structure of a message forwarded to the dead letter box."""

    original_notification: str
    job: str
    description: str
    exception_class: str
    remark: str | None = None
    timestamp: float = field(default_factory=time.time)
