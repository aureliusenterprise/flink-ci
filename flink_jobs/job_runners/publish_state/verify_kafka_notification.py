from typing import cast

from marshmallow import ValidationError
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import ProcessFunction

from flink_jobs.job_runners import AtlasProcessFunction

from .model import KafkaNotification, ValidatedInput

# Constants for job name and error tags
JOB_NAME = "publish_state"
SCHEMA_ERROR_TAG = OutputTag("dead_letter")
NO_ENTITY_ERROR_TAG = OutputTag("no_entity")


class ValidateKafkaNotifications(AtlasProcessFunction):
    """
    ProcessFunction for input validation on Kafka Notifications.

    Attributes
    ----------
    main : DataStream
        The main data stream that contains validated notifications.
    schema_validation_errors : DataStream
        A side output stream that captures schema validation errors.
    no_entity_errors : DataStream
        A side output stream that captures errors related to missing entities.
    errors : DataStream
        A unified stream of all validation errors for consolidated handling.
    """

    def __init__(self, input_stream: DataStream) -> None:
        """
        Initialize the ValidateKafkaNotifications object.

        This method sets up the main output stream based on the given input stream,
        and also configures any side outputs.

        Parameters
        ----------
        input_stream : DataStream
            The input DataStream to be processed.
        """
        super().__init__(input_stream, JOB_NAME)

        # Set up the main data stream
        self.main = (
            self.input_stream
            .process(self)
            .name("validated_notifications")
        )

        # Capture schema validation errors as a side output
        self.schema_validation_errors = (
            self.main
            .get_side_output(SCHEMA_ERROR_TAG)
            .name("schema_errors")
        )

        # Capture errors related to missing entities as a side output
        self.no_entity_errors = (
            self.main
            .get_side_output(NO_ENTITY_ERROR_TAG)
            .name("no_entity_errors")
        )

        # Union of all error streams for consolidated error handling or logging
        self.errors = (
            self.schema_validation_errors
            .union(self.no_entity_errors)
            .name("input_validation_errors")
        )

    def process_element(
        self,
        value: str,
        _: ProcessFunction.Context | None = None,
    ) -> ValidatedInput | tuple[OutputTag, Exception]:
        """
        Process and validate the content of each Kafka notification.

        The input validation step includes the following checks:

        1. Ensures the message adheres to the schema.
        2. Checks the presence of required content in the message.

        Parameters
        ----------
        value : str
            The input value, which is a serialized JSON string representing the Kafka notification.
        _ : ProcessFunction.Context, optional
            The context for the ProcessFunction. This is not used in this function.

        Returns
        -------
        ValidatedInput or tuple[OutputTag, Exception]
            - `ValidatedInput` object on the main output if the input is valid.
            - If the input doesn't match the schema, return a `ValidationError` on the error output.
            - If the input doesn't include an entity, return a `ValueError` on the error output.
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
