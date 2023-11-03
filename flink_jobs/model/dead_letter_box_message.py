from dataclasses import dataclass  # noqa: INP001

from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json

"""
This data class "DeadLetterBoxMessage" describes the structure of a message forwarded to the
dead letter box.
This is a Kafka topic which contains a message for each input notification that could not be
handled by any of the jobs implemented.
"""
@dataclass_json(letter_case=LetterCase.CAMEL) # type: ignore
@dataclass
class DeadLetterBoxMesage(DataClassJsonMixin):
    """
    DeadLetterBoxMessage.

    Representation of an exception as a data structure to be published in the Kafka Dead letter
    box.
    """

    def __init__(self, timestamp: int,  # noqa: PLR0913
                        original_notification: str,
                        job: str,
                        description: str,
                        exception_class: str,
                        remark: str) -> None:
        """
        Initialize the DeadLetterBoxMesage.

        Parameters
        ----------
        timestamp : int
            The timestamp of the original event
        original_notification : str
            The original event, which could not be processed
        job : str
            The job, which raised the exception
        description: str
            Description of the meaning of the exception with some indication what went wrong
        exception_class : str
            Name of the exception class
        remark : str
            Space for custome remarks.
        """
        self.timestamp = timestamp
        self.original_notification = original_notification
        self.job = job
        self.description = description
        self.exception_class = exception_class
        self.remark = remark
