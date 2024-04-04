from collections.abc import Generator, Iterator
from dataclasses import dataclass, field

from elasticsearch import ApiError, Elasticsearch
from elasticsearch.helpers import ScanError, scan

from flink_tasks import AppSearchDocument


@dataclass
class TransactionError(Exception):
    """
    Base class for exceptions raised by the Transaction class.

    Parameters
    ----------
    transaction : Transaction
        The transaction instance that raised the error.
    """

    transaction: "Transaction"


@dataclass
class FindOneError(TransactionError):
    """
    Exception raised when an error occurs while fetching a single document from Elasticsearch.

    Parameters
    ----------
    guid : str
        The GUID of the document that could not be fetched.
    """

    guid: str


@dataclass
class FindManyError(TransactionError):
    """
    Exception raised when an error occurs while fetching multiple documents from Elasticsearch.

    Parameters
    ----------
    query : dict
        The query dictionary that caused the error during fetching.
    """

    query: dict


@dataclass
class Transaction:
    """
    A class for managing transactions with Elasticsearch, providing methods to commit, delete, and find documents.

    Use the Transaction class to manage the state of documents retrieved from Elasticsearch, avoiding inconsistencies
    when documents are read and updated multiple times.

    Attributes
    ----------
    elastic : Elasticsearch
        An instance of Elasticsearch client.
    index_name : str
        The name of the Elasticsearch index to operate on.
    """

    elastic: Elasticsearch
    index_name: str

    _updated_documents: dict[str, AppSearchDocument | None] = field(default_factory=dict, init=False)

    def commit(self, document: AppSearchDocument) -> None:
        """
        Commit a document to the transaction, marking it for later update or addition.

        Parameters
        ----------
        document : AppSearchDocument
            The document to be committed.
        """
        self._updated_documents[document.guid] = document

    def delete(self, guid: str) -> None:
        """
        Mark a document identified by its GUID for deletion.

        Parameters
        ----------
        guid : str
            The GUID of the document to be marked for deletion.
        """
        self._updated_documents[guid] = None

    def find_one(self, guid: str, *, commit: bool = False) -> AppSearchDocument | None:
        """
        Find a single document by its GUID, optionally committing it to the transaction.

        Returns None if no document is found or if the document is marked for deletion.

        Parameters
        ----------
        guid : str
            The GUID of the document to find.
        commit : bool, optional
            Whether to commit the found document to the transaction (default is False).

        Returns
        -------
        AppSearchDocument | None
            The found document, or None if the document is either not found or marked for deletion.

        Raises
        ------
        FindOneError
            If an error occurs while fetching the document from Elasticsearch.
        """
        if guid in self._updated_documents:
            return self._updated_documents[guid]

        try:
            result = self.elastic.get(index=self.index_name, id=guid)
        except ApiError as error:
            raise FindOneError(self, guid) from error

        if not result["found"]:
            return None

        document = AppSearchDocument.from_dict(result["_source"])

        if commit:
            self.commit(document)

        return document

    def find_many(self, query: dict, *, commit: bool = False) -> Generator[AppSearchDocument, None, None]:
        """
        Find multiple documents matching the given query, optionally committing them to the transaction.

        Please note that documents marked for deletion will not be included in the results.

        Parameters
        ----------
        query : dict
            The query dictionary to match documents against.
        commit : bool, optional
            Whether to commit the found documents to the transaction (default is False).

        Yields
        ------
        AppSearchDocument
            A generator yielding the found documents.

        Raises
        ------
        FindManyError
            If an error occurs while fetching the documents from Elasticsearch.
        """
        try:
            for result in scan(self.elastic, index=self.index_name, query=query):
                document = AppSearchDocument.from_dict(result["_source"])

                if document.guid in self._updated_documents:
                    committed = self._updated_documents[document.guid]

                    if committed is None:
                        continue

                    document = committed

                if commit:
                    self.commit(document)

                yield document
        except ScanError as error:
            raise FindManyError(self, query) from error

    def __iter__(self) -> Iterator[tuple[str, AppSearchDocument | None]]:
        """
        Iterate over all documents currently managed by the transaction.

        Yields
        ------
        tuple[str, AppSearchDocument | None]
            An iterator over tuples of document GUIDs and the corresponding AppSearchDocument objects or None.
        """
        yield from self._updated_documents.items()
