import pytest
from types import GeneratorType

from flink_jobs.dead_letter_wrapper import ErrorHandler, DeadLetterErrorHandler


def test_origin_beahvior():
    """
    Test that wraper did't change return
    """
    handler = ErrorHandler(error_tag="error_tag_1")

    def func(a=1):
        return a**2
    wrapped_func = handler(func)

    assert func(1) == next(wrapped_func(1))
    assert func(3) == next(wrapped_func(3))
    assert func(-5) == next(wrapped_func(-5))    

def test_origin_beahvior_2():
    """
    Test that wraper did't change return
    """
    handler = DeadLetterErrorHandler(error_tag="error_tag_3", job="test_job_3")

    def func(a=1):
        return f'asd{a-3}'
    wrapped_func = handler(func)

    assert func(5) == next(wrapped_func(5))
    assert func(13) == next(wrapped_func(13))
    assert func(-15) == next(wrapped_func(-15))

def test_deadletter_handler():
    """
    Test that wraper handle error correctly
    """
    handler = DeadLetterErrorHandler(error_tag="error_tag_4", job="test_job_4")

    def func(kafka_notification):
        1/0
    wrapped_func = handler(func)

    result = wrapped_func(kafka_notification="some kafka_notification")
    assert isinstance(result, GeneratorType)
    first = next(result)
    assert isinstance(first, tuple)
    assert len(first) == 2
    assert first[0] == "error_tag_4"
    dead_letter = first[1]
    assert dead_letter.job == "test_job_4"
    assert dead_letter.original_notification == "some kafka_notification"
    assert dead_letter.exception_class == "ZeroDivisionError"

def test_deadletter_handler_no_kafka_notification():
    """
    Test that wraper handle error correctly
    """
    handler = DeadLetterErrorHandler(error_tag="error_tag_5", job="test_job_5")

    def func():
        1/0
    wrapped_func = handler(func)

    result = wrapped_func()
    assert isinstance(result, GeneratorType)
    first = next(result)
    assert isinstance(first, tuple)
    assert len(first) == 2
    assert first[0] == "error_tag_5"
    dead_letter = first[1]
    assert dead_letter.job == "test_job_5"
    assert dead_letter.original_notification == "None"
    assert dead_letter.exception_class == "ZeroDivisionError"