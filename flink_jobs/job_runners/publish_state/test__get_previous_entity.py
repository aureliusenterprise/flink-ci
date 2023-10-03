from collections.abc import Generator
from unittest import mock

import pytest
from pyflink.datastream import OutputTag

from flink_jobs.elastic_client import ElasticClient, ElasticPreviousStateRetrieveError


@pytest.fixture()
def elastic_client() -> Generator[ElasticClient, None, None]:

    client = mock.Mock(spec=ElasticClient)

    expected_document = {"guid": "asd"}

    with mock.patch.object(client, "get_previous_atlas_entity", return_value=expected_document):
        yield client

@pytest.fixture()
def elastic_client_raises_error() -> Generator[ElasticClient, None, None]:

    client = mock.Mock(spec=ElasticClient)

    expected_error = ElasticPreviousStateRetrieveError(
        guid="asd",
        creation_time=1,
    )

    with mock.patch.object(client, "get_previous_atlas_entity", side_effect=expected_error):
        yield client

@pytest.fixture()
def error_tag() -> OutputTag:
    return OutputTag("dead_letter")



