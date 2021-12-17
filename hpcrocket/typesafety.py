from typing import Optional, Type, TypeVar, Union


T = TypeVar("T")
Raisable = Union[Exception, Type[Exception]]


def get_or_raise(instance: Optional[T], exception: Raisable) -> T:
    if instance is None:
        raise exception

    return instance
