from pyflink.datastream import DataStream, MapFunction, OutputTag

from flink_tasks import AtlasChangeMessageWithPreviousVersion, EntityMessage

from .event_handlers import EVENT_HANDLERS

UNKNOWN_EVENT_TYPE_TAG = OutputTag("unknown_event_type")
DETERMINE_CHANGE_ERROR_TAG = OutputTag("determine_change_error")


class DetermineChangeFunction(MapFunction):
    """
    A Flink MapFunction that processes incoming messages to identify changes within a data stream.

    It parses the messages into a structured `AtlasChangeMessageWithPreviousVersion` format,
    validates them, and applies the appropriate event handler based on the operation type of the
    message. Errors during processing are tagged and sent to designated side outputs for error
    handling.
    """

    def map(
        self,
        value: AtlasChangeMessageWithPreviousVersion,
    ) -> list[EntityMessage] | tuple[OutputTag, Exception]:
        """
        Process the incoming message to determine changes using predefined event handlers.

        Parameters
        ----------
        value : AtlasChangeMessageWithPreviousVersion
            The incoming change message.

        Returns
        -------
        list[EntityMessage] | tuple[OutputTag, Exception]
            Returns a list of `EntityMessage` if changes are successfully determined, or a tuple
            containing `OutputTag` and `Exception` if an error occurs during processing.
        """
        operation_type = value.message.operation_type

        if operation_type not in EVENT_HANDLERS:
            message = f"Unknown event type: {operation_type}"
            return UNKNOWN_EVENT_TYPE_TAG, NotImplementedError(message)

        event_handler = EVENT_HANDLERS[operation_type]

        messages = []

        try:
            messages = event_handler(value)
        except ValueError as e:
            return DETERMINE_CHANGE_ERROR_TAG, e

        return messages


class DetermineChange:
    """
    Manages the process of identifying changes in a Flink data stream.

    Sets up a pipeline for applying `DetermineChangeFunction` to each message in the incoming data
    stream, organizing the output into the main data stream and side outputs for errors.

    Attributes provide access to the main data stream and the side outputs, facilitating separate
    downstream processing for change data and various errors.
    """

    def __init__(self, data_stream: DataStream) -> None:
        """
        Initialize the DetermineChange object with a data stream and prepare the pipeline.

        The initialization process maps the data stream through the `DetermineChangeFunction` and
        establishes side outputs for validation errors, unknown event types, and other errors
        encountered during change determination.

        Parameters
        ----------
        data_stream : DataStream
            The incoming Flink data stream with serialized change messages.
        """
        self.data_stream = data_stream

        self.changes = self.data_stream.map(DetermineChangeFunction()).name("determine_change")

        self.unknown_event_types = self.changes.get_side_output(UNKNOWN_EVENT_TYPE_TAG).name(
            "unknown_event_types",
        )

        self.determine_change_errors = self.changes.get_side_output(
            DETERMINE_CHANGE_ERROR_TAG,
        ).name("determine_change_errors")

        self.main = self.changes.flat_map(
            lambda messages: (message for message in messages),
        ).name("determine_change_results")

        self.errors = self.unknown_event_types.union(
            self.determine_change_errors,
        )
