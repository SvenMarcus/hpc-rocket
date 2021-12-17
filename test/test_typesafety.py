import pytest

from hpcrocket.typesafety import get_or_raise


def test__given_instance_and_exception__should_return_instance():
    a_string = "A_string"

    actual = get_or_raise(a_string, RuntimeError())

    assert actual is a_string


def test__given_none_and_exception__should_raise_exception():
    with pytest.raises(RuntimeError):
        get_or_raise(None, RuntimeError())


def test__given_false_boolean_and_exception__should_return_false():
    a_bool = False

    actual = get_or_raise(a_bool, RuntimeError())

    assert actual is False