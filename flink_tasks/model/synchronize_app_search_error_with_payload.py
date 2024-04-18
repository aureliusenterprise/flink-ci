from flink_tasks import SynchronizeAppSearchError


class SynchronizeAppSearchErrorWithPayload(SynchronizeAppSearchError):
    """Exception raised when elastic search results are not full, but contain partial results."""
    def __init__(self, message, partial_result=None):
        super().__init__(message)
        self.partial_result = partial_result if partial_result is not None else []

    def __str__(self):
        return f'{super().__str__()}, Partial result: {self.partial_result}'
