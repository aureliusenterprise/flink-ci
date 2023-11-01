import asyncio
from collections.abc import Callable

from aiohttp.web import HTTPError
from keycloak import KeycloakError, KeycloakOpenID
from m4i_atlas_core import AtlasChangeMessage, get_entity_by_guid
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import MapFunction

# Define output tags for errors that can occur during processing.
NO_ENTITY_ERROR_TAG = OutputTag("no_entity")
ENTITY_LOOKUP_ERROR_TAG = OutputTag("entity_lookup_error")

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
    """

    def __init__(self, keycloak_factory: KeycloakFactory, credentials: tuple[str, str]) -> None:
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

    def open(self) -> None:  # noqa: A003
        """Initialize the keycloak instance using the provided keycloak factory."""
        self.keycloak = self.keycloak_factory()

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
        change_message = AtlasChangeMessage.from_json(value)
        entity = change_message.message.entity

        if entity is None:
            return NO_ENTITY_ERROR_TAG, ValueError("No entity found in message.")

        try:
            entity_details = asyncio.run(
                get_entity_by_guid(
                    guid=entity.guid,
                    access_token=self.access_token,
                    cache_read=False,
                ),
            )
        except (HTTPError, KeycloakError) as e:
            return ENTITY_LOOKUP_ERROR_TAG, e

        change_message.message.entity = entity_details

        return change_message

    @property
    def access_token(self) -> str:
        """
        Get the current access token using the Keycloak client.

        Returns
        -------
        str
            The access token.
        """
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
        keycloak_factory: KeycloakFactory,
        credentials: tuple[str, str],
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

        self.errors = self.entity_lookup_errors.union(self.no_entity_errors)
