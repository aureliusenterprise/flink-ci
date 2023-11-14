import asyncio  # noqa: INP001

from m4i_atlas_core import (
    ConfigStore,
    connectors_types_def,
    create_type_defs,
    data_dictionary_types_def,
    kubernetes_types_def,
    process_types_def,
)

store = ConfigStore.get_instance()

store.load({
    "atlas.credentials.username": "admin",
    "atlas.credentials.password": "admin",
    #"atlas.server.url": os.getenv('ATLAS_EXTERNAL_URL') + "/api/atlas",
    "atlas.server.url": "http://atlas:21000/api/atlas",
})

# creation of type deinitions breaks at the moment because the m4i_atlas_core handle_requests
# is only capable of sending http requests to port 80
asyncio.run(create_type_defs(data_dictionary_types_def))
asyncio.run(create_type_defs(process_types_def))
asyncio.run(create_type_defs(connectors_types_def))
asyncio.run(create_type_defs(kubernetes_types_def))
