class ElasticPersistingError(Exception):
    """Exception raised when storing state in ElasticSearch fails for a given document ID."""

    def __init__(self, doc_id: str, stage:str) -> None:
        """
        Initialize the ElasticPersistingException exception.

        Parameters
        ----------
        doc_id : str
            The ID of the document for which storing state failed.
        """
        message = f"Storing state failed for doc_id {doc_id} in stage {stage}"
        super().__init__(message)

        self.doc_id = doc_id
        self.stage = stage


class ElasticPreviousStateRetrieveError(Exception):
    """Exception raised for errors in the retrieval of previous state from ElasticSearch."""

    def __init__(self, guid: str, creation_time: int, stage:str) -> None:
        """
        Initialize the ElasticPreviousStateRetrieveError exception.

        Parameters
        ----------
        guid : str
            The GUID for which the retrieval of previous state failed.
        creation_time : int
            The creation time for which the retrieval of previous state failed.
        """
        message = f"Failed to retrieve pervious state for guid {guid} and time {creation_time} in stage {stage}"  # noqa: E501
        super().__init__(message)

        self.guid = guid
        self.creation_time = creation_time
        self.stage = stage
