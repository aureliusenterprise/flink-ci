import os

import requests
from elastic_app_search import Client
from requests.auth import HTTPBasicAuth

from app_search_engine_setup import engines

enterprise_search_url = os.getenv('ENTERPRISE_SEARCH_EXTERNAL_URL')
elastic_username = os.getenv('ELASTIC_USERNAME')
elastic_password = os.getenv('ELASTIC_PASSWORD')


def get_enterprise_api_private_key():
    key_response = requests.get(
        f'{enterprise_search_url}api/as/v1/credentials/private-key',
        auth=HTTPBasicAuth(elastic_username, elastic_password)
    )
    key_info = key_response.json()
    return key_info['key']


enterprise_search_endpoint = f'{enterprise_search_url[8:]}api/as/v1'
api_key = get_enterprise_api_private_key()

kub_client = Client(
    base_endpoint=enterprise_search_endpoint,
    api_key=api_key,
    use_https=True
)

for engine in engines:
    kub_client.create_engine(engine['name'])
    kub_client.update_schema(engine['name'], engine['schema'])
    kub_client.update_search_settings(engine['name'], engine['search-settings'])
