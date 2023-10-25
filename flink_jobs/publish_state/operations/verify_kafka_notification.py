from typing import cast

from marshmallow import ValidationError
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import MapFunction

from flink_jobs.publish_state import KafkaNotification, ValidatedInput

SCHEMA_ERROR_TAG = OutputTag("dead_letter")
NO_ENTITY_ERROR_TAG = OutputTag("no_entity")

class ValidationFunction(MapFunction):
    """
    A custom MapFunction to validate incoming messages from a Kafka stream.

    The function attempts to deserialize the input JSON string into a KafkaNotification
    object and then validates the deserialized message. Errors in the deserialization
    process or missing entity values in the message are forwarded to side outputs.
    """

    def map( # noqa: A003
        self,
        value: str,
    ) -> ValidatedInput | tuple[OutputTag, Exception]:
        """
        Deserialize and validate the input message.

        Parameters
        ----------
        value : str
            The input JSON string.

        Returns
        -------
        ValidatedInput or tuple[OutputTag, Exception]
            ValidatedInput object if the message is valid, else a tuple containing an
            OutputTag indicating the error type and the exception raised.
        """
        try:
            # Deserialize the JSON string into a KafkaNotification object.
            # Using `cast` due to a known type hinting issue with schema.loads
            message_object = cast(
                KafkaNotification,
                KafkaNotification.schema().loads(value, many=False),
            )
        except ValidationError as e:
            return SCHEMA_ERROR_TAG, e

        entity = message_object.kafka_notification.message.entity

        if entity is None:
            return NO_ENTITY_ERROR_TAG, ValueError("No entity found in message.")

        return ValidatedInput(
            entity,
            message_object.event_time,
            message_object.msg_creation_time,
        )

class ValidateKafkaNotifications:
    """
    A class that sets up the Flink data streams for validating Kafka notifications.

    This class initializes the data streams, applies the validation logic, and
    organizes the output into `main`, `schema_errors`, `no_entity_errors`, and `errors` streams.

    Attributes
    ----------
    input_stream : DataStream
        The input stream of Kafka notifications.
    main : DataStream
        The main output stream containing validated messages.
    schema_errors : DataStream
        The side output stream for messages that failed schema validation.
    no_entity_errors : DataStream
        The side output stream for messages without entities.
    errors : DataStream
        The union of schema_errors and no_entity_errors.
    """

    def __init__(self, input_stream: DataStream) -> None:
        self.input_stream = input_stream

        # Set up the main data stream
        self.main = (
            self.input_stream
            .map(ValidationFunction())
            .name("validated_notifications")
        )

        self.schema_errors = (
            self.main
            .get_side_output(SCHEMA_ERROR_TAG)
            .name("schema_errors")
        )

        self.no_entity_errors = (
            self.main
            .get_side_output(NO_ENTITY_ERROR_TAG)
            .name("no_entity_errors")
        )

        self.errors = self.schema_errors.union(self.no_entity_errors)
