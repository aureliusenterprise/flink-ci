import asyncio
from collections.abc import Callable
from typing import cast

from aiohttp.web import HTTPError
from keycloak import KeycloakError, KeycloakOpenID
from m4i_atlas_core import AtlasChangeMessage, get_entity_by_guid
from marshmallow import ValidationError
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import MapFunction, RuntimeContext

# Define output tags for errors that can occur during processing.
ENTITY_LOOKUP_ERROR_TAG = OutputTag("entity_lookup_error")
NO_ENTITY_ERROR_TAG = OutputTag("no_entity")
SCHEMA_ERROR_TAG = OutputTag("schema_error")

# A type alias for a factory function that produces instances of KeycloakOpenID.
KeycloakFactory = Callable[[], KeycloakOpenID]


class GetEntityFunction(MapFunction):
    """
    A PyFlink map function that enriches an AtlasChangeMessage with entity details.

    If the entity is missing or there's an HTTP error during the enrichment, it outputs an
    error message to a side output. Utilizes a Keycloak instance to manage authentication tokens.

    Attributes
    ----------
    keycloak_factory : KeycloakFactory
        A factory function to produce instances of KeycloakOpenID.
    credentials : tuple[str, str]
        A tuple containing the client_id and client_secret for authentication.
    keycloak : KeycloakOpenID
        The Keycloak instance used for token management.
    loop : asyncio.AbstractEventLoop
        The event loop used for asynchronous tasks.
    """

    def __init__(self, keycloak_factory: KeycloakFactory|None,
                    credentials: tuple[str, str]) -> None:
        """
        Initialize the GetEntityFunction with a Keycloak factory and credentials.

        Parameters
        ----------
        keycloak_factory : KeycloakFactory
            A factory function to produce instances of KeycloakOpenID.
        credentials : tuple[str, str]
            A tuple containing the client_id and client_secret for authentication.
        """
        self.credentials = credentials
        self.keycloak_factory = keycloak_factory

    def open(self, runtime_context: RuntimeContext) -> None:  # noqa: A003, ARG002
        """Initialize the keycloak instance using the provided keycloak factory."""
        if self.keycloak_factory is not None:
            self.keycloak = self.keycloak_factory()
        self.loop = asyncio.new_event_loop()

    def close(self) -> None:
        """Close the event loop."""
        self.loop.close()

    def map(self, value: str) -> AtlasChangeMessage | tuple[OutputTag, Exception]:  # noqa: A003
        """
        Process the incoming message and enrich it with entity details.

        Parameters
        ----------
        value : str
            The input message in JSON format.

        Returns
        -------
        AtlasChangeMessage
            If the message is successfully enriched.
        tuple[OutputTag, Exception]
            If there's an error during processing.
        """
        try:
            # Deserialize the JSON string into a KafkaNotification object.
            # Using `cast` due to a known type hinting issue with schema.loads
            change_message = cast(
                AtlasChangeMessage,
                AtlasChangeMessage.schema().loads(value, many=False),
            )
        except ValidationError as e:
            return SCHEMA_ERROR_TAG, e

        entity = change_message.message.entity

        if entity is None:
            return NO_ENTITY_ERROR_TAG, ValueError("No entity found in message.")

        try:
            entity_details = self.loop.run_until_complete(
                get_entity_by_guid(
                    guid=entity.guid,
                    access_token=self.access_token,
                    cache_read=False,
                ),
            )
        except (HTTPError, KeycloakError) as e:
            return ENTITY_LOOKUP_ERROR_TAG, RuntimeError(str(e))

        change_message.message.entity = entity_details

        return change_message

    @property
    def access_token(self) -> str|None:
        """
        Get the current access token using the Keycloak client.

        Returns
        -------
        str
            The access token.
        """
        if self.keycloak is None:
            return None
        return self.keycloak.token(*self.credentials)["access_token"]


class GetEntity:
    """
    A class to handle the data stream and process it using the GetEntityFunction.

    This class initializes the main data stream, processes it, and handles errors by
    directing them to side outputs.

    Attributes
    ----------
    data_stream : DataStream
        The main data stream to be processed.
    main : DataStream
        The main data stream after processing with GetEntityFunction.
    entity_lookup_errors : DataStream
        Data stream for entity lookup errors.
    no_entity_errors : DataStream
        Data stream for messages with no entity.
    errors : DataStream
        Combined data stream of all errors.
    """

    def __init__(
        self,
        data_stream: DataStream,
        credentials: tuple[str, str],
        keycloak_factory: KeycloakFactory | None = None,
    ) -> None:
        """
        Initialize the GetEntity class with a given data stream.

        Parameters
        ----------
        data_stream : DataStream
            The input data stream to be processed.
        """
        self.data_stream = data_stream

        self.main = self.data_stream.map(GetEntityFunction(keycloak_factory, credentials)).name(
            "enriched_entities",
        )

        self.entity_lookup_errors = self.main.get_side_output(ENTITY_LOOKUP_ERROR_TAG).name(
            "entity_lookup_errors",
        )

        self.no_entity_errors = self.main.get_side_output(NO_ENTITY_ERROR_TAG).name(
            "no_entity_errors",
        )

        self.schema_errors = self.main.get_side_output(SCHEMA_ERROR_TAG).name("schema_errors")

        self.errors = self.entity_lookup_errors.union(self.no_entity_errors, self.schema_errors)
