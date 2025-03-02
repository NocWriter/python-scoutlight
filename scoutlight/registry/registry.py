from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple, List, Dict, Union
from rolodex.discovery.exceptions import DiscoveryException


class KeyDoesNotExist(DiscoveryException):
    """
    Indicates that query a key that does not exist.
    """
    pass


class Registry(object):
    """
        A strategy design pattern used for registering and querying services in a repository.
        """
    __metaclass__ = ABCMeta

    def __init__(self):
        # type: () -> None
        """
        Class initializer.
        """
        pass

    def setup(self):
        """
        A callback issued by the framework on a strategy during setup time to perform all required setup steps.
        """
        pass

    def destroy(self):
        """
        A callback issued by the framework during shutdown.

        Typically, the strategy should release all resources and deregister all local services.
        """
        pass

    @abstractmethod
    def put_if_not_exist(self, key, value):
        # type: (str, str) -> bool
        """
        Set a key's value if key is not defined yet.

        :param key: Key to set.
        :param value: Value to set. Must be a valid string.
        :return: True if the key was set, False otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def put(self, key, value):
        """
        Set (if not already set) a value for a given key.
        :param key: Key to set.
        :param value: Value to set. Must be a valid string.
        """
        raise NotImplementedError()

    @abstractmethod
    def put_all(self, values):
        # type: (Union[Dict[str, Optional[str]], Tuple[str, Optional[str]], List[Tuple[str, Optional[str]]]]) -> None
        """
        Set a group of values atomically. Either all values are set or none is set.

        :param values: Set of values. It can be either a dictionary, a single tuple (key/value pair) or a list of
                       tuples (a set of key/value pairs).
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, key):
        # type: (str) -> Optional[str]
        """
        Fetch a value associated with a key. 
        :param key: Key to fetch.
        :return: Key value.
        :raises KeyDoesNotExist: If key does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def list_keys(self, parent_key, recursive=False, keep_order=False):
        # type: (str, bool, bool) -> List[str]
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
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, parent_key, recursive=False, keep_order=False):
        # type: (str, bool, bool) -> Dict[str, str]
        """
        Fetch all children keys and values for a given parent.

        If 'recursive' is set to True, then all children at all levels are returned.
        If 'recursive' is set to False, only immediate children are returned.

        :param parent_key: Parent key to fetch children keys for.
        :param recursive: True to recursively fetch children keys, False to fetch only immediate children.
        :param keep_order: If set to True, the returned dictionary will retain the original order of the children,
                           (sorted by creation) using OrderedDict.
                           If set to False, the returned dictionary may not retain the original order of the keys
                           (use Python standard dictionary), however, the model may be more efficient.
                           Typically useful for large datasets.
        :return: A dictionary holding all keys and their values.
        :raises KeyDoesNotExist: If the parent key does not exist.
        """
        raise NotImplementedError()
