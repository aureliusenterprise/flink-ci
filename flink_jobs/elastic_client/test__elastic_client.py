from unittest import mock

from elasticsearch import Elasticsearch

from .elastic_client import ElasticClient
from .model import ElasticSearchEntity


def test__search_atlas_entity_query_format() -> None:
    """
    Test the query formation for searching the atlas entity in ElasticSearch.

    This test ensures that the query and sort parameters used for searching the ElasticSearch are
    correctly formed. Mocks are used to ensure that the `search` method is called with the expected
    arguments.
    """
    elasticsearch = mock.Mock(spec=Elasticsearch)
    client = ElasticClient(lambda: elasticsearch, atlas_entities_index="test")

    expected_query = {
        "bool": {
            "filter": [
                {
                    "match": {
                        "body.guid.keyword": "asd",
                    },
                },
                {
                    "range": {
                        "msgCreationTime": {
                            "lt": 1,
                        },
                    },
                },
            ],
        },
    }

    expected_sort = {
        "msgCreationTime": {"numeric_type": "long", "order": "desc"},
    }

    with mock.patch.object(elasticsearch, "search") as patched_search:
        client.get_previous_atlas_entity("asd", 1)
        patched_search.assert_called_once_with(
            index="test",
            query=expected_query,
            sort=expected_sort,
            size=1,
        )


def test__search_atlas_entity_with_result() -> None:
    """
    Test that the function correctly returns a found entity.

    This test checks that when a search query returns a result,
    the `get_previous_atlas_entity` method returns the expected document.
    """
    elasticsearch = mock.Mock(spec=Elasticsearch)
    client = ElasticClient(lambda: elasticsearch, atlas_entities_index="test")

    expected_document = {"guid": "asd"}

    mock_search_result = {
        "hits": {
            "total": {
                "value": 1,
            },
            "hits": [
                {
                    "_source": {
                        "body": expected_document,
                    },
                },
            ],
        },
    }

    with mock.patch.object(elasticsearch, "search", return_value=mock_search_result):
        result = client.get_previous_atlas_entity("asd", 1)
        assert result == expected_document


def test__search_atlas_entity_no_result() -> None:
    """
    Test the function behavior when no entity is found.

    This test ensures that `get_previous_atlas_entity` returns `None`
    when the search query does not find any entity.
    """
    elasticsearch = mock.Mock(spec=Elasticsearch)
    client = ElasticClient(lambda: elasticsearch, atlas_entities_index="test")

    mock_search_result = {
        "hits": {
            "total": {
                "value": 0,
            },
            "hits": [],
        },
    }

    with mock.patch.object(elasticsearch, "search", return_value=mock_search_result):
        result = client.get_previous_atlas_entity("asd", 1)
        assert result is None


def test__index_atlas_entity() -> None:
    """
    Test the indexing of an atlas entity in ElasticSearch.

    This test checks that the `index` method is called with the expected arguments
    when indexing an atlas entity in Elasticsearch.
    """
    elasticsearch = mock.Mock(spec=Elasticsearch)
    client = ElasticClient(lambda: elasticsearch, atlas_entities_index="test")

    mock_entity = mock.Mock(spec=ElasticSearchEntity)

    expected_document = {
        "msgCreationTime": 1,
        "eventTime": 2,
        "body": mock_entity,
    }

    with mock.patch.object(elasticsearch, "index") as patched_index:
        client.index_atlas_entity("asd", 1, 2, mock_entity)
        patched_index.assert_called_once_with(
            index="test",
            id="asd_1",
            document=expected_document,
        )
