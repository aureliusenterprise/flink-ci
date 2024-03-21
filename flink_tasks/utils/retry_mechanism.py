import functools
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, TypeVar

T = TypeVar("T")

logger = logging.getLogger("retry")


class RetryError(Exception):
    """Exception raised when the maximum number of retries is exceeded."""

    def __init__(self, attempts: int, message: str = "Max retries exceeded") -> None:
        self.attempts = attempts
        super().__init__(f"{message}. Attempts: {attempts}")


class RetryStrategy(Protocol):
    """Protocol for defining retry strategies."""

    def sleep(self, attempts: int) -> None:
        """Sleep for a duration based on the retry strategy."""


@dataclass
class FixedDelay(RetryStrategy):
    """Simple retry strategy with a fixed delay."""

    delay: float = 1

    def sleep(self, attempts: int) -> None:  # noqa: ARG002
        """Sleep for a fixed duration."""
        time.sleep(self.delay)


@dataclass
class ExponentialBackoff(RetryStrategy):
    """Exponential backoff retry strategy."""

    initial_delay: float = 1
    multiplier: float = 2
    ceil: float = 60
    jitter: tuple[float, float] = (0, 1)

    def __post_init__(self) -> None:
        if self.initial_delay <= 0:
            message = "Initial delay must be greater than 0"
            raise ValueError(message)
        if self.multiplier <= 1:
            message = "Multiplier must be greater than 1"
            raise ValueError(message)
        if self.ceil <= 0:
            message = "Ceil must be greater than 0"
            raise ValueError(message)
        if not (0 <= self.jitter[0] < self.jitter[1]):
            message = "Jitter must be a tuple with 0 <= min < max"
            raise ValueError(message)

    def sleep(self, attempts: int) -> None:
        """Sleep for a duration based on the exponential backoff strategy."""
        current_delay = functools.reduce(
            lambda x, _: x * self.multiplier + random.uniform(*self.jitter),  # noqa: S311
            range(attempts),
            self.initial_delay,
        )
        time.sleep(min(current_delay, self.ceil))


def retry(  # noqa: ANN201
    retry_strategy: RetryStrategy,
    catch: tuple[type[Exception], ...] = (Exception,),
    max_retries: int = 5,
):
    """Retry decorator with a configurable retry strategy."""

    def decorator(func: Callable[..., T]):  # noqa: ANN202
        @functools.wraps(func)
        def wrapper(*args: ..., **kwargs: ...) -> T:
            attempts = 0
            while attempts < max_retries:
                try:
                    return func(*args, **kwargs)
                except catch as e:  # noqa: PERF203
                    attempts += 1
                    logger.warning("Attempt %d failed", attempts, exc_info=True)
                    if attempts == max_retries:
                        raise RetryError(attempts) from e
                    retry_strategy.sleep(attempts)
            raise RetryError(attempts)

        return wrapper

    return decorator
