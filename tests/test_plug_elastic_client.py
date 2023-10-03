from typing import Any

from flink_jobs.errors import (
    ElasticPersistingError,
    ElasticPreviousStateRetrieveError,
    ElasticSearchResultSchemeError,
)


class TestPlugElasticClient:
    _fail = False
    _results = {}
    _saved_income = None

    def set_fail(self, fail: bool):
        self._fail = fail

    def set_result(self, method: str, method_kwargs: dict, method_result: Any):
        if method not in self._results:
            self._results[method] = []

        self._results[method].append(
            {
                "method_kwargs": method_kwargs,
                "method_result": method_result,
            },
        )

    def get_previous_atlas_entity(self, entity_guid: str, msg_creation_time: int) -> dict:
        if self._fail:
            msg = f"Failed to retrieve perviouse state for guid {entity_guid} at msg_creation_time {msg_creation_time}"
            raise ElasticPreviousStateRetrieveError(msg)
        for result in self._results.get("get_previous_atlas_entity", []):
            if {"entity_guid": entity_guid, "msg_creation_time": msg_creation_time} == result["method_kwargs"]:
                return result["method_result"]
        msg = "Wrong search result scheme "
        raise ElasticSearchResultSchemeError(msg)

    def index(self, entity_guid: str, msg_creation_time: int, event_time: int, atlas_entity: dict) -> bool:
        doc_id = f"{entity_guid}_{msg_creation_time}"

        document = {
            "msgCreationTime": msg_creation_time,
            "eventTime": event_time,
            "body": atlas_entity,
        }
        if self._fail:
            raise ElasticPersistingError
        for result in self._results.get("index", []):
            if {"entity_guid": entity_guid, "msg_creation_time": msg_creation_time, "event_time": event_time, "atlas_entity": atlas_entity} == result["method_kwargs"]:
                self._saved_income = {"id": doc_id, "document": document}
                return result["method_result"]
        raise ElasticPersistingError
