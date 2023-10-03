from functools import wraps
from types import GeneratorType

from dataclasses_json import DataClassJsonMixin


def dataclass_serialiser(method: callable) -> callable:
    """
    decorator generator for Pyflink Functions as Process or Map

    serialize returning object using to_json method of dataclasses, also proceed return with flink tags

    In case if result is generator, serialize each yield
    """
    @wraps(method)
    def wrapped(*args, **kwargs):
        result = method(*args, **kwargs)

        # TODO: maybe this should be an option
        to_json_if_it_exists = lambda obj: obj.to_json() if "to_json" in  dir(obj) else obj
        
        # TODO: to complicated and specific
        if not isinstance(result, GeneratorType):
            if result is None:
                return None
            elif isinstance(result, tuple):
                return result[0], to_json_if_it_exists(result[1])
            else:
                return to_json_if_it_exists(result)
            
        for res in result:
            if res is None:
                yield None
            elif isinstance(res, tuple):
                yield res[0], to_json_if_it_exists(res[1])
            else:
                yield to_json_if_it_exists(res)
    return wrapped


def dataclass_deserializer(dataclass_to_serialize: DataClassJsonMixin) -> callable:
    """
    decorator generator for Pyflink Functions as Process or Map

    deserialize income of function from string to dataclass_to_serialize
    """
    def wrapper(method: callable) -> callable:
        @wraps(method)
        # TODO: somthing with contex
        def wrapped(self, value, ctx=None):
            return method(self, dataclass_to_serialize.from_json(value))
        return wrapped
    return wrapper