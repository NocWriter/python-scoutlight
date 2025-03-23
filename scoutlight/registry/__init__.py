from abc import ABCMeta, abstractmethod

from typing import Optional, Tuple, List, Dict, Union

from scoutlight.exceptions import DiscoveryException
from scoutlight.registry.key import Key
from scoutlight.tools.lifecycle import Lifecycle


class KeyDoesNotExist(DiscoveryException):
    """
    Indicates that query a key that does not exist.
    """
    pass


class Registry(Lifecycle):
    """
        A strategy design pattern used for registering and querying services in a repository.
        """
    __metaclass__ = ABCMeta

    def __init__(self):
        # type: () -> None
        """
        Class initializer.
        """
        super(Registry, self).__init__()

    def setup(self):
        """
        A callback issued by the framework on a strategy during setup time to perform all required setup steps.
        """
        super(Registry, self).setup()

    def destroy(self):
        """
        A callback issued by the framework during shutdown.

        Typically, the strategy should release all resources and deregister all local services.
        """
        super(Registry, self).destroy()

    def put_if_not_exist(self, k, value):
        # type: (Union[Key, str], str) -> bool
        """
        Set a key's value if key is not defined yet.

        :param k: Key to set.
        :param value: Value to set.
                      Must be a valid string.
        :return: True if the key was set, False otherwise.
        """
        k = self._to_key(k)
        self._assert_value(value)

        return self._put((k, value), k)

    def put(self, k, v):
        # type: (Union[Key, str], str) -> None
        """
        Set (if not already set) a value for a given key.
        :param k: Key to set.
        :param v: Value to set.
        """
        k = self._as_key(k)
        self._assert_value(v)

        return self._put([(k, v)])

    # noinspection StructuralWrap
    def put_all(self, values):
        # type: (Union[Dict[Union[Key, str], str], Tuple[Union[Key, str], str], List[Tuple[Union[Key, str], str]]]) -> None
        """
        Set a group of values atomically.
        Either all values are set or none is set.

        :param values: Set of values.
                       It can be either a dictionary, a single tuple (key/value pair) or a list of
                       tuples (a set of key/value pairs).
        """
        kv_list = self._to_kv_list(values)
        self._put(kv_list)

    def get(self, k):
        # type: (Key) -> str
        """
        Fetch a value associated with a key.
        :param k: Key to fetch.
        :return: Key value.
        :raises KeyDoesNotExist: If key does not exist.
        """
        return self._get_one(self._as_key(k))

    def list_keys(self, parent_key, recursive=False, keep_order=False):
        # type: (Union[Key, str], bool, bool) -> List[str]
        """
        Fetch all children keys for a given parent.

        If 'recursive' is set to True, then all children at all levels are returned.
        If 'recursive' is set to False, only immediate children are returned.

        If 'keep_order' is set to True, then the returned list of keys is sorted by creation date.

        :param parent_key: Parent key to fetch children keys for.
        :param recursive: True to recursively fetch children keys, False to fetch only immediate children.
        :param keep_order: True to return the list in order of creation, False otherwise.
        :return: List of keys under given parent.
        :raises KeyDoesNotExist: If the parent key does not exist.
        """
        return list(self._get(self._as_key(parent_key), recursive, keep_order, True, True).keys())

    def fetch(self, parent_key, recursive=False, keep_order=False):
        # type: (Union[Key, str], bool, bool) -> Dict[str, str]
        """
        Fetch all children keys and values for a given parent.

        If 'recursive' is set to True, then all children at all levels are returned.
        If 'recursive' is set to False, only immediate children are returned.

        :param parent_key: Parent key to fetch children keys for.
        :param recursive: True to recursively fetch children keys, False to fetch only immediate children.
        :param keep_order: If set to True, the returned dictionary will retain the original order of the children
                           (sorted by creation) using OrderedDict.
                           If set to False, the returned dictionary may not retain the original order of the keys
                           (use Python standard dictionary), however, the model may be more efficient.
                           Typically useful for large datasets.
        :return: A dictionary holding all keys and their values.
        :raises KeyDoesNotExist: If the parent key does not exist.
        """
        return self._get(self._as_key(parent_key), recursive, keep_order, False, True)

    @abstractmethod
    def _put(self, kv_list, conditional_key_exist=None):
        # type: (List[Tuple[Key]], Optional[Key]) -> bool
        """
        Set one or more key(s)/value(s).
        The put operation can be condition, i.e., set only if a given key exists.

        :param kv_list: List of Key/value pair(s).
        :param conditional_key_exist: If defined (non-None), the key/value pairs are set only if the given key does not
                                      exist.
        :return: True if the key/value pairs were set, False otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_one(self, get_key):
        # type: (Key) -> str
        """
        Fetch a single key from the store.

        This should be an optimal operation with as less overhead as possible.

        :param get_key: Key to fetch data for.
        :return: Value associated with the given key.
        :raises KeyDoesNotExist: If the key does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def _get(self, get_key, recursive=False, keep_order=False, keys_only=False, exclude_parent_keys=True):
        # type: (Key, bool, bool, bool, bool) -> Dict[Key, str]
        """
        Fetch one or more values from the store.

        The operation supports multiple criteria.
            - If keep_order is True, then the returned value is an OrderedDict with all key/value pairs sorted by
              creation date, in ascending order.
            - If keys_only is set to True, the dictionary includes only keys.
              Values are set to empty strings and
              should be ignored by caller.
            - If exclude_parent_keys is set to True, then the returned value will not include the 'get_key' and its
              value.
              If False, the returned dictionary will include it.

        :param get_key: Key to fetch.
        :param recursive: True to recursively fetch all keys and, optionally, values under given 'get_key'.
                          False to only fetch the requested key.
        :param keep_order: True to retain the order of the keys as they were originally created.
                           False to return in any given order.
        :param keys_only: True to fetch only the keys.
                          False to fetch both keys and values.
        :param exclude_parent_keys: If True, the returned result will not include the parent key (i.e.: get_key).
                                    False will cause the inclusion of the parent key.
        :return: A dictionary with key/value pair(s).
        :raises KeyDoesNotExist: If the key does not exist.
        """
        raise NotImplementedError()

    @staticmethod
    def _as_key(key_or_string):
        # type: (Union[Key, str]) -> Key
        """
        Return a given object as a Key.

        If the given parameter is a string, it is converted to a Key.
        If the given parameter is already a Key, it is returned as it is.

        :param key_or_string: Object to be converted to Key.
        :return: A Key object.
        :raises AssertionError: If provided parameter is neither a string nor a Key.
        """

        if isinstance(key_or_string, Key):
            # Do nothing. It's already a Key.
            result = key_or_string
        elif isinstance(key_or_string, str):
            result = Key.create(key_or_string)
        else:
            raise AssertionError(
                "Unsupported key type: {}. A key must be either a string or Key object.".format(type(key_or_string)))

        return result

    @staticmethod
    def _assert_value(value):
        # type: (str) -> None
        """
        Assert that a given value is valid (string type).

        :param value: Value to examine.
        :raises AssertionError: If value is not valid.
        """
        assert isinstance(value, basestring), "Invalid value (must be a string type, got {} instead.).".format(
            type(value))

    # noinspection StructuralWrap
    def _to_tuple_list(self, values):
        # type: (Union[Dict[Union[Key, str], Optional[str]], Tuple[Union[Key, str], str], List[Tuple[Union[Key, str], str]]]) -> List[Tuple[Key, str]]
        """
        Convert an input value that may be a single tuple, list of tuples or a dictionary to a list of tuples,
        where each tuple is a key/value pair.

        :param values: Values to convert to a tuple.
        :return: List of key/value pairs as tuples.
        :raises TypeError: If values are neither of: tuple, list of tuples, dictionary.
        :raises AssertionError: If either keys or values are not of string type or key is an empty string.
        """
        pair_list = []  # type: List[Tuple[Key, str]]

        def append_pair(_k, _v):
            _k = self._to_key(_k)
            self._assert_value(_v)
            pair_list.append((_k, _v))

        if isinstance(values, dict):
            for k, v in values.items():
                append_pair(k, v)
        elif isinstance(values, tuple):
            assert len(values) == 2, "Invalid tuple length (must be 2 -- key/value pair)."
            append_pair(values[0], values[1])
        elif isinstance(values, list):
            for list_item in values:
                assert isinstance(list_item, tuple) and len(
                    list_item) == 2, "Values list contains a non-tuple key/value pair."
                append_pair(values[0], values[1])
        else:
            raise TypeError("Unsupported input type: {}.".format(type(values)))

        return pair_list
