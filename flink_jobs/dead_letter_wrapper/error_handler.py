import logging
import time
import traceback
from collections.abc import Callable
from functools import wraps
from types import GeneratorType
from typing import TypeVar

from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag

from flink_jobs.flink_dataclasses import DeadLetterBoxMesage

DEFAULT_TAG = OutputTag("error", Types.STRING())

R = TypeVar("R")
Wrappable = Callable[..., R]

class ErrorHandler:
    """
    A parameterizable error handling decorator designed for use with PyFlink processors.

    It is used for catching errors during the execution of the original `process_element` and
    yield processed error with a Flink tag which can be used for filtering results as a side Output.

    In case of no error, it yields the original return.

    Attributes
    ----------
    _error_tag : OutputTag
        The OutputTag that will be added to the error message.
    """

    def __init__(self, error_tag: OutputTag = DEFAULT_TAG) -> None:
        """
        Initialize an instance of ErrorHandler.

        Parameters
        ----------
        error_tag : OutputTag, optional
            OutputTag will be added to error message, by default OutputTag("error", Types.STRING())
        """
        self._error_tag = error_tag

    def _cook_error_message(self, error: Exception, method_args: list | None = None, method_kwargs: dict | None = None) -> dict:
        """
        Prepare the error message.

        Parameters
        ----------
        error : Exception
            The exception that was raised.
        method_args : list, optional
            Arguments of the original process_element method's call which failed, by default [].
        method_kwargs : dict, optional
            Keyword arguments of the original process_element method's call which failed, by default {}.

        Returns
        -------
        dict
            The prepared error message as a dictionary.
        """
        if method_kwargs is None:
            method_kwargs = {}
        if method_args is None:
            method_args = []
        return {"error": str(error)}

    def __call__(self, method: Wrappable[R]) -> Wrappable[R]:
        """
        Wrap the method with error handling. It makes the instance callable, used for decorating the method.

        Parameters
        ----------
        method : Wrappable
            The method to be decorated.

        Returns
        -------
        Wrappable
            The decorated method.
        """

        @wraps(method)
        def wrapper(*args, **kwargs) -> R | tuple[OutputTag, dict]:
            try:
                result = method(*args, **kwargs)
                if isinstance(result, GeneratorType):
                    yield from result
                else:
                    yield result
            except Exception as e:  # noqa: BLE001
                yield self._error_tag, self._cook_error_message(error=e, method_args=args, method_kwargs=kwargs)

        return wrapper


class DeadLetterErrorHandler(ErrorHandler):
    """
    DeadLetterErrorHandler callable class desined for parametrizable decoration of process_element method for pyflink ProcessFunction class
    Based on ErrorHandler class
    Catch error durring original process_element method call and yield provided flink tag with processed error as DeadLetterBoxMesage object
    which can be used for filtering result as side Output
    In case of no error yield original return.
    """

    # TODO: what is going on??
    _KAFKA_NOTIFICATION_ARG_NAME = "kafka_notification"

    def __init__(self, error_tag: OutputTag = OutputTag("error", Types.STRING()), job: str = "flink_job") -> None:
        """
        Parameters
        ----------
        error_tag (OutputTag): flink tag will be added to error message
        job (str): name of original job (will be added to final message).
        """
        self._error_tag = error_tag
        self._job = job

    def _cook_error_message(self, error: Exception, exc_info: tuple, method_args: list | None = None, method_kwargs: dict | None = None) -> DeadLetterBoxMesage:
        """
        Prepare error message.

        Parameters
        ----------
                error (Exception):
                exc_info (tuple): (type(e), e, e.__traceback__)
                method_args (list): args of original process_element methods call which faild
                method_kwargs (dict): kwargs of original process_element methods call which faild

        Returns
        -------
                error_message (json)
        """
        if method_kwargs is None:
            method_kwargs = {}
        if method_args is None:
            method_args = []
        if self._KAFKA_NOTIFICATION_ARG_NAME in method_kwargs:
            kafka_notification = method_kwargs.get(
                self._KAFKA_NOTIFICATION_ARG_NAME, "")
        elif len(method_args) > 1:
            kafka_notification = method_args[1]
        else:
            # TODO: what need to do here?
            kafka_notification = "None"
            logging.warning(
                f"DeadLetterErrorHandler: wrapper excpected {self._KAFKA_NOTIFICATION_ARG_NAME} in wrapped method, but didn't recieved it")

        return DeadLetterBoxMesage(
            timestamp=time.time(), original_notification=kafka_notification, job=self._job,
            description=("".join(traceback.format_exception(*exc_info))), exception_class=type(error).__name__, remark=None)
