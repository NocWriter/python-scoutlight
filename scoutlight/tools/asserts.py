from typing import Any


def is_none_or_instance(obj, clazz):
    # type: (Any, type) -> bool
    """
    Test if a given object is either None or is of the given class.

    :param obj: Object to check.
    :param clazz: The type of class the object is expected to be.
    :return: True if 'obj' is None or instance of clazz, False otherwise.
    """
    return obj is None or isinstance(obj, clazz)


def is_none_or_str(obj):
    # type: (Any) -> bool
    """
    Test if a given object is either None or is of the given string.

    :param obj: Object to test.
    :return: True if 'obj' is None or a string, False otherwise.
    """
    return is_none_or_instance(obj, basestring)


def assert_none_or_string(obj, parameter_name):
    # type: (Any, str) -> None
    """
    Assert if a given object, which is considered a function parameter, is either None or is of the given string.
    If not, raise an exception.

    :param obj: Object to test.
    :param parameter_name: Name of parameter to include in an error message.
    :raises: AssertionError if 'obj' is None or a string, False otherwise.
    """
    assert is_none_or_str(obj), "'{}' must be either None or a string type.".format(parameter_name)
