import asyncio
import contextlib
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import cast

from aiohttp.web import HTTPError
from m4i_atlas_core import (
    AtlasChangeMessage,
    ConfigStore,
    Entity,
    EntityAuditAction,
    ExistingEntityTypeException,
    data_dictionary_entity_types,
    get_entity_by_guid,
    register_atlas_entity_types,
)
from marshmallow import ValidationError
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.functions import MapFunction, RuntimeContext

from flink_tasks.utils import ExponentialBackoff, retry
from keycloak import KeycloakError, KeycloakOpenID

# Define output tags for errors that can occur during processing.
ENTITY_LOOKUP_ERROR_TAG = OutputTag("entity_lookup_error")
NO_ENTITY_ERROR_TAG = OutputTag("no_entity")
SCHEMA_ERROR_TAG = OutputTag("schema_error")


ENTITY_CREATE_TAG = OutputTag("entity_create")
ENTITY_UPDATE_TAG = OutputTag("entity_update")
ENTITY_DELETE_TAG = OutputTag("entity_delete")

# A type alias for a factory function that produces instances of KeycloakOpenID.
KeycloakFactory = Callable[[], KeycloakOpenID]


class SplitByEventType(MapFunction):
    """A PyFlink map function that directs incoming messages to side outputs by their operation type."""

    def map(self, value: AtlasChangeMessage) -> None | tuple[OutputTag, AtlasChangeMessage]:
        """Direct the incoming message to the appropriate side output based on the operation type."""
        if value.message.operation_type == EntityAuditAction.ENTITY_CREATE:
            return ENTITY_CREATE_TAG, value
        if value.message.operation_type == EntityAuditAction.ENTITY_UPDATE:
            return ENTITY_UPDATE_TAG, value
        if value.message.operation_type == EntityAuditAction.ENTITY_DELETE:
            return ENTITY_DELETE_TAG, value
        return None


class GetEntityFunction(MapFunction):
    """
    A PyFlink map function that enriches an AtlasChangeMessage with entity details.

    If the entity is missing or there's an HTTP error during the enrichment, it outputs an
    error message to a side output. Utilizes a Keycloak instance to manage authentication tokens.

    Attributes
    ----------
    atlas_url : str
        The URL of the Apache Atlas API.
    keycloak_factory : KeycloakFactory
        A factory function to produce instances of KeycloakOpenID.
    credentials : tuple[str, str]
        A tuple containing the client_id and client_secret for authentication.
    keycloak : KeycloakOpenID
        The Keycloak instance used for token management.
    loop : asyncio.AbstractEventLoop
        The event loop used for asynchronous tasks.
    """

    def __init__(
        self,
        atlas_url: str,
        keycloak_factory: KeycloakFactory,
        credentials: tuple[str, str],
    ) -> None:
        """
        Initialize the GetEntityFunction with a Keycloak factory and credentials.

        Parameters
        ----------
        atlas_url : str
            The URL of the Apache Atlas API.
        keycloak_factory : KeycloakFactory
            A factory function to produce instances of KeycloakOpenID.
        credentials : tuple[str, str]
            A tuple containing the client_id and client_secret for authentication.
        """
        self.atlas_url = atlas_url
        self.credentials = credentials
        self.keycloak_factory = keycloak_factory

        self._access_token = None
        self._token_expiration = datetime.now(tz=timezone.utc)

    def open(self, runtime_context: RuntimeContext) -> None:  # noqa: ARG002
        """Initialize the keycloak instance using the provided keycloak factory."""
        self.keycloak = self.keycloak_factory()
        self.loop = asyncio.new_event_loop()

        store = ConfigStore.get_instance()
        store.set("atlas.server.url", self.atlas_url)

        with contextlib.suppress(ExistingEntityTypeException):
            register_atlas_entity_types(data_dictionary_entity_types)

    def close(self) -> None:
        """Close the event loop."""
        self.loop.close()

    def map(self, value: str) -> AtlasChangeMessage | tuple[OutputTag, Exception]:
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
            logging.exception("Error deserializing message")
            return SCHEMA_ERROR_TAG, e

        logging.debug("Successfully deserialized message: %s", change_message)

        entity = change_message.message.entity

        if entity is None:
            logging.error("No entity found in message: %s", change_message)
            return NO_ENTITY_ERROR_TAG, ValueError(f"No entity found in message. Value={value}")

        if change_message.message.operation_type == EntityAuditAction.ENTITY_DELETE:
            return change_message

        try:
            entity_details = self.get_entity(entity.guid, entity.type_name)
        except HTTPError as e:
            logging.exception("HTTP error during entity lookup")
            return ENTITY_LOOKUP_ERROR_TAG, RuntimeError(f"HTTP error during entity lookup: {e}")
        except KeycloakError as e:
            logging.exception("Auth error during entity lookup")
            return ENTITY_LOOKUP_ERROR_TAG, e

        change_message.message.entity = entity_details

        logging.debug("Successfully enriched change message: %s", change_message)

        return change_message

    @retry(retry_strategy=ExponentialBackoff(), catch=(HTTPError, KeycloakError))
    def get_entity(self, guid: str, entity_type: str) -> Entity:
        """
        Get the entity details for the given GUID and entity type.

        Parameters
        ----------
        guid : str
            The GUID of the entity to fetch.
        entity_type : str
            The type of the entity to fetch.

        Returns
        -------
        Entity
            The entity details.
        """
        return self.loop.run_until_complete(
            get_entity_by_guid(
                guid=guid,
                entity_type=entity_type,
                access_token=self.access_token,
                cache_read=False,
            ),
        )

    @property
    def access_token(self) -> str:
        """
        Get the current access token using the Keycloak client.

        If the token has expired or is not set, a new token is fetched.

        The token is considered expired before its actual expiration time by a buffer to ensure that
        operations using the token do not fail due to a token that expires mid-operation. The buffer
        is set to 80% of the actual token's lifespan.

        Returns
        -------
        str
            The access token.
        """
        now = datetime.now(tz=timezone.utc)
        # If the token is expired or about to expire, fetch a new one
        if now > self._token_expiration or self._access_token is None:
            token_response = self.keycloak.token(*self.credentials)

            # Calculate the expiration time with some buffer (80% of the actual expiration)
            expires_in = int(token_response["expires_in"])
            self._token_expiration = now + timedelta(seconds=expires_in * 0.8)

            self._access_token = token_response["access_token"]

        return self._access_token


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
        atlas_url: str,
        keycloak_factory: KeycloakFactory,
        credentials: tuple[str, str],
    ) -> None:
        """
        Initialize the GetEntity class with a given data stream.

        Parameters
        ----------
        data_stream : DataStream
            The input data stream to be processed.
        atlas_url : str
            The URL of the Apache Atlas API.
        keycloak_factory : KeycloakFactory
            A factory function to produce instances of KeycloakOpenID.
        """
        self.data_stream = data_stream

        self.main = self.data_stream.map(
            GetEntityFunction(atlas_url, keycloak_factory, credentials),
        ).name("enriched_entities")

        split_by_event_type = self.main.map(SplitByEventType()).name("split_by_event_type")

        self.entity_create = split_by_event_type.get_side_output(ENTITY_CREATE_TAG).name("entity_create")
        self.entity_update = split_by_event_type.get_side_output(ENTITY_UPDATE_TAG).name("entity_update")
        self.entity_delete = split_by_event_type.get_side_output(ENTITY_DELETE_TAG).name("entity_delete")

        self.entity_lookup_errors = self.main.get_side_output(ENTITY_LOOKUP_ERROR_TAG).name(
            "entity_lookup_errors",
        )

        self.no_entity_errors = self.main.get_side_output(NO_ENTITY_ERROR_TAG).name(
            "no_entity_errors",
        )

        self.schema_errors = self.main.get_side_output(SCHEMA_ERROR_TAG).name("schema_errors")

        self.errors = self.entity_lookup_errors.union(self.no_entity_errors, self.schema_errors)
