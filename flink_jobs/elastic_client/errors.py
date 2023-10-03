class ElasticPersistingError(Exception):
    """Exception raised when storing state in ElasticSearch fails for a given document ID."""

    def __init__(self, doc_id: str) -> None:
        """
        Initialize the ElasticPersistingException exception.

        Parameters
        ----------
        doc_id : str
            The ID of the document for which storing state failed.
        """
        message = f"Storing state failed for doc_id {doc_id}"
        super().__init__(message)

        self.doc_id = doc_id


class ElasticPreviousStateRetrieveError(Exception):
    """Exception raised for errors in the retrieval of previous state from ElasticSearch."""

    def __init__(self, guid: str, creation_time: int) -> None:
        """
        Initialize the ElasticPreviousStateRetrieveError exception.

        Parameters
        ----------
        guid : str
            The GUID for which the retrieval of previous state failed.
        creation_time : int
            The creation time for which the retrieval of previous state failed.
        """
        message = f"Failed to retrieve pervious state for guid {guid} and time {creation_time}"
        super().__init__(message)

        self.guid = guid
        self.creation_time = creation_time
