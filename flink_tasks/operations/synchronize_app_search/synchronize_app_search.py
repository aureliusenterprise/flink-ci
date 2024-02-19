from collections.abc import Callable

from elasticsearch import Elasticsearch
from pyflink.datastream import DataStream, MapFunction, OutputTag, RuntimeContext

from flink_tasks import (
    AppSearchDocument,
    EntityMessage,
    EntityMessageType,
    SynchronizeAppSearchError,
)

from .event_handlers import (
    handle_delete_breadcrumbs,
    handle_delete_derived_entities,
    handle_entity_created,
    handle_update_attributes,
    handle_update_breadcrumbs,
    handle_update_derived_entities,
)

ElasticsearchFactory = Callable[[], Elasticsearch]
EventHandler = Callable[[EntityMessage, Elasticsearch, str], list[AppSearchDocument]]

EVENT_HANDLERS: dict[EntityMessageType, list[EventHandler]] = {
    EntityMessageType.ENTITY_CREATED: [handle_entity_created],
    EntityMessageType.ENTITY_ATTRIBUTE_AUDIT: [
        handle_update_attributes,
        handle_update_breadcrumbs,
        handle_update_derived_entities,
    ],
    EntityMessageType.ENTITY_DELETED: [handle_delete_breadcrumbs, handle_delete_derived_entities],
}

UNKNOWN_EVENT_TYPE_TAG = OutputTag("unknown_event_type")
SYNCHRONIZE_APP_SEARCH_ERROR_TAG = OutputTag("synchronize_app_search_error")


class SynchronizeAppSearchFunction(MapFunction):
    """
    Updates Elasticsearch with the changes in the incoming data stream.

    Attributes
    ----------
    elastic_factory : ElasticsearchFactory
        A factory function that returns an Elasticsearch client instance.
    index_name : str
        The name of the index in Elasticsearch to which data is synchronized.
    """

    def __init__(self, elastic_factory: ElasticsearchFactory, index_name: str) -> None:
        """
        Initialize the SynchronizeAppSearchFunction.

        Parameters
        ----------
        elastic_factory : ElasticsearchFactory
            A factory function that returns an Elasticsearch client instance.
        index_name : str
            The name of the index in Elasticsearch to which data is synchronized.
        """
        self.elastic_factory = elastic_factory
        self.index_name = index_name

    def open(self, runtime_context: RuntimeContext) -> None:  # noqa: ARG002
        """
        Initialize the Elasticsearch client. This method is called when the function is opened.

        Parameters
        ----------
        runtime_context : RuntimeContext
            The runtime context in which the function is executed.
        """
        self.elastic = self.elastic_factory()

    def map(
        self,
        value: EntityMessage,
    ) -> list[tuple[str, AppSearchDocument | None]] | tuple[OutputTag, Exception]:
        """
        Process an EntityMessage and perform actions based on the type of change event.

        Parameters
        ----------
        value : EntityMessage
            The message to be processed.

        Returns
        -------
        list[tuple[str, AppSearchDocument | None]] | tuple[OutputTag, Exception]
            A list of tuples containing document GUIDs and documents, or a tuple of an OutputTag and
            an Exception.
        """
        result: list[tuple[str, AppSearchDocument | None]] = []

        event_type = value.event_type

        if event_type not in EVENT_HANDLERS:
            message = f"Unknown event type: {event_type}"
            return UNKNOWN_EVENT_TYPE_TAG, NotImplementedError(message)

        event_handlers = EVENT_HANDLERS[event_type]

        try:
            result.extend(
                (doc.guid, doc)
                for handler in event_handlers
                for doc in handler(value, self.elastic, self.index_name)
            )

            if event_type == EntityMessageType.ENTITY_DELETED:
                result.append((value.guid, None))
        except SynchronizeAppSearchError as e:
            return SYNCHRONIZE_APP_SEARCH_ERROR_TAG, e

        return result

    def close(self) -> None:
        """Close the Elasticsearch client. This method is called when the function is closed."""
        self.elastic.close()


class SynchronizeAppSearch:
    """
    Sets up a data stream for synchronizing App Search documents based on incoming change events.

    Attributes
    ----------
    data_stream : DataStream
        The data stream that contains change messages.
    app_search_documents : DataStream
        The processed stream of App Search documents as a batch per change.
    unknown_event_types : DataStream
        The stream containing messages with unknown event types.
    synchronize_app_search_errors : DataStream
        The stream containing synchronization errors.
    main : DataStream
        The stream of individual App Search documents.
    errors : DataStream
        The unified stream of errors.
    """

    def __init__(
        self,
        data_stream: DataStream,
        elastic_factory: ElasticsearchFactory,
        index_name: str,
    ) -> None:
        self.data_stream = data_stream

        self.app_search_documents = self.data_stream.map(
            SynchronizeAppSearchFunction(elastic_factory, index_name),
        ).name("synchronize_app_search")

        self.unknown_event_types = self.app_search_documents.get_side_output(
            UNKNOWN_EVENT_TYPE_TAG,
        ).name("unknown_event_types")

        self.synchronize_app_search_errors = self.app_search_documents.get_side_output(
            SYNCHRONIZE_APP_SEARCH_ERROR_TAG,
        ).name("synchronize_app_search_errors")

        self.main = self.app_search_documents.flat_map(
            lambda documents: (document for document in documents),
        ).name("app_search_documents")

        self.errors = self.unknown_event_types.union(self.synchronize_app_search_errors)
