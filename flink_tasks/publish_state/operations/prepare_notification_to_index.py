

from pyflink.datastream import DataStream
from pyflink.datastream.functions import MapFunction

from flink_tasks.publish_state import EntityVersion, ValidatedInput


# There is a typo in the name
class PrepareNotificationToIndexFunction(MapFunction):
    """
    A custom `MapFunction` to prepare notifications for indexing.

    The function processes the validated input and transforms it into an `EntityVersion`
    object suitable for indexing.
    """

    def map(self, value: ValidatedInput) -> EntityVersion: # noqa: A003
        """
        Transform a ValidatedInput message into an EntityVersion object.

        Parameters
        ----------
        value : ValidatedInput
            The validated input message to be prepared for indexing.

        Returns
        -------
        EntityVersion
            The transformed message, ready for indexing.
        """
        msg_creation_time = value.msg_creation_time
        event_time = value.event_time

        doc_id = f"{value.entity.guid}_{msg_creation_time}"

        return EntityVersion(
            value.entity,
            doc_id,
            event_time,
            msg_creation_time,
        )

class PrepareNotificationToIndex:
    """
    A class that sets up the Flink data stream for preparing notifications for indexing.

    This class initializes the data stream and applies the transformation logic using
    `e` to produce messages ready for indexing.

    Attributes
    ----------
    input_stream : DataStream
        The input stream of validated messages.
    main : DataStream
        The main output stream containing messages prepared for indexing.
    """

    def __init__(self, input_stream: DataStream) -> None:
        """
        Initialize the PrepareNotificationToIndex with an input data stream.

        Parameters
        ----------
        input_stream : DataStream
            The input stream of validated notifications.
        """
        self.input_stream = input_stream

        self.main = (
            self.input_stream
            .map(PrepareNotificationToIndexFunction())
            .name("index_preparation")
        )
