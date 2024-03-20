import random
import time
from collections.abc import Callable
from typing import TypeVar

from elasticsearch import Elasticsearch

T = TypeVar("T")

class MaxRetriesReachedError(Exception):
    """Exception raised when the maximum number of retries is reached."""

    def __init__(self, attempts: int, message: str="Maximum retry attempts reached") -> None:
        self.attempts = attempts
        super().__init__(f"{message}. Attempts: {attempts}")


class PredicateNotMetError(Exception):
    """Exception raised when the predicate condition is not met after all retries."""

    def __init__(self, attempts: int, message: str="Predicate not met after maximum retry attempts") -> None:
        self.attempts = attempts
        super().__init__(f"{message}. Attempts: {attempts}")


def retry_with_backoff(  # noqa: PLR0913
        function: Callable[[list[str], Elasticsearch, str], T],
        ids: list[str],
        elastic: Elasticsearch,
        index_name: str,
        max_retries: int=5,
        predicate: Callable[[T], bool] | None = None,
        ) -> T:
    """Retry a function with exponential backoff."""
    retry_delay = 1
    for attempt in range(max_retries):
        try:
            result = function(ids, elastic, index_name)
            if predicate is None or predicate(result):
                return result
        except MaxRetriesReachedError as e:
            if attempt == max_retries - 1:
                raise MaxRetriesReachedError(attempt) from e
        # Exponential backoff
        time.sleep(retry_delay)
        retry_delay *= 2
        retry_delay += random.uniform(0, 1)  # noqa: S311

    raise PredicateNotMetError(attempt)
