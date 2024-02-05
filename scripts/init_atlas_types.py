"""Creates all types required for Aurelius Atlas."""
import asyncio
import logging
import os

import aiohttp
from m4i_atlas_core import (
    ConfigStore,
    create_type_defs,
    data_dictionary_types_def,
)


async def main() -> None:
    """Create the types in Atlas."""
    try:
        types_def = await create_type_defs(data_dictionary_types_def)
        logging.info("Types created: %s", types_def.to_json())
    except aiohttp.ClientResponseError as err:
        if err.status == 409: # noqa: PLR2004
            logging.warning("Types already exist. Skipping creation.")

if __name__ == "__main__":
    store = ConfigStore.get_instance()

    store.load({
        "atlas.credentials.username": os.environ.get("KEYCLOAK_USERNAME", "admin"),
        "atlas.credentials.password": os.environ.get("KEYCLOAK_PASSWORD", "admin"),
        "atlas.server.url": "http://localhost:21000/api/atlas",
    })

    asyncio.run(main())
