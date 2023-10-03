from abc import ABC
from typing import cast

from marshmallow import ValidationError
from pyflink.datastream import DataStream, OutputTag, ProcessFunction

from .model import EntityVersion, KafkaNotification

ERROR_TAG = OutputTag("errors")
LOG_TAG = OutputTag("logs")


class Operation(ProcessFunction, ABC):

    def __init__(self, input_stream: DataStream) -> None:
        self.input_stream = input_stream

        self.main = input_stream \
            .process(self) \
            .filter(lambda value: value is not None)

        self.errors = self.main.get_side_output(ERROR_TAG)
        self.logs = self.input_stream


class ValidateInputOperation(Operation):

    def __init__(self, input_stream: DataStream) -> None:
        super().__init__(input_stream)
        self.main = self.main.filter(lambda value: value is not None)

    def process_element(
        self,
        value: str,
        ctx: ProcessFunction.Context | None = None,
    ) -> EntityVersion | tuple[OutputTag, Exception] | None:
        try:
            notification = KafkaNotification.schema().loads(value, many=False)
            notification = cast(KafkaNotification, notification)
        except ValidationError as e:
            return ERROR_TAG, e

        entity = notification.kafka_notification.message.entity

        if entity is None:
            return None

        event_time = notification.event_time
        msg_creation_time = notification.msg_creation_time

        doc_id = f"{entity.guid}_{msg_creation_time}"

        return EntityVersion(
            entity,
            doc_id,
            event_time,
            msg_creation_time,
        )
