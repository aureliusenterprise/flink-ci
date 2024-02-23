"""Creates all types required for Aurelius Atlas."""
import os

from m4i_atlas_core import (
    ConfigStore,
    get_keycloak_token,
)

if __name__ == "__main__":
    store = ConfigStore.get_instance()

    store.load(
        {
            "keycloak.client.id": os.environ.get("KEYCLOAK_CLIENT_ID"),
            "keycloak.credentials.username": os.environ.get("KEYCLOAK_USERNAME", "admin"),
            "keycloak.credentials.password": os.environ.get("KEYCLOAK_PASSWORD", "admin"),
            "keycloak.realm.name": os.environ.get("KEYCLOAK_REALM_NAME"),
            "keycloak.client.secret.key": os.environ.get("KEYCLOAK_CLIENT_SECRET_KEY"),
            "keycloak.server.url": "http://localhost:8180/auth/",

        },
    )

    print(get_keycloak_token()) #noqa: T201
