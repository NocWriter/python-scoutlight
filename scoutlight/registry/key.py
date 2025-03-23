from typing import List, Union

from scoutlight.exceptions import DiscoveryException
from scoutlight.tools.key_tools import normalize_key, construct_key


class KeyException(DiscoveryException):
    pass


class Key(object):
    """
    A Key object represents a service discovery key for a service.
    A key is composed of parts, separated via forward slashes, e.g.: '/my/service/demo' represents a key with the
    parts ['my', 'service', 'demo'].

    The '/my' and '/my/service' both considered parents of 'demo'.
    The '/my' is considered as a general parent of 'demo' and '/my/service' is considered as 'immediate parent' of
    'demo'.

    Prefer using the Key.create(...) factory method over the classic __init__ invocation.
    The __init__ may change over time, however, the Key.create(...) will retain its signature.
    """

    def __init__(self, key, key_parts):
        # type: (str, List[str]) -> None
        """
        Class initializer.

        :param key: The key full key as a string.
        :param key_parts: All key parts.
        """
        self._key = key  # type: str
        self._key_parts = key_parts  # type: List[str]

    @classmethod
    def create(cls, key):
        """
        Factory method for creating a new Key object. This is the preferred way of creating a Key object.

        :param key: The key string to create a new Key object.
        :return: The new Key object.
        """
        key = normalize_key(key)
        return Key(key, key.split('/')[1:])

    @property
    def key(self):
        # type: () -> str
        """
        :return: The key as a string.
        """
        return self._key

    @property
    def key_length(self):
        # type: () -> int
        """
        :return: The length of the key (number of parts in the key).
        """
        return len(self._key_parts)

    def get_parent(self):
        # type: () -> Key
        """
        :return: The parent key of this key.
        """
        parent = self._key_parts[:-1]  # type: List[str]
        return Key(construct_key(parent), parent)

    def relative(self, relative_path):
        # type: (Union[Key, str]) -> Key
        """
        Creates a new key which is relative to this key.

        For example, if this key is '/my/service' and the relative key is 'print_service/queue', the newly created key
        will be '/my/service/printer_service/queue'.

        :param relative_path: A relative key, which may be either a string or another key.
        :return: A new Key object.
        """
        all_parts = list(self._key_parts)
        if isinstance(relative_path, basestring):
            all_parts.append(relative_path)
        elif isinstance(relative_path, Key):
            all_parts.extend(relative_path._key_parts)
        else:
            raise TypeError("Unsupported type: {} (must be either string or Key).".format(type(relative_path)))
        new_key = construct_key(*all_parts)  # type: str
        return Key.create(new_key)

    def is_a_parent(self, parent):
        # type: (Key) -> bool
        """
        Test if a given key is a parent of this key.

        A parent key is any key which part of this key.
        For example, '/my' and '/my/service' are both parents of '/my/service/demo'.

        :param parent: Key to test as a parent key.
        :return: True if this key is a parent of this key, False otherwise.
        """
        assert isinstance(parent, Key), "Parameter must be of type 'Key'."

        return parent.key_length < self.key_length and self.__starts_with(parent._key_parts)

    def is_immediate_parent(self, parent):
        # type: (Key) -> bool
        """
        Test if a given key is an immediate parent of this key.

        An immediate parent is one that shares all parts, excluding the last one.
        For example, '/my/service' is an immediate parent of '/my/service/demo'.
        However, '/my' is not an immediate parent of '/my/service/demo'.

        :param parent: Key to test as a parent key.
        :return: True if this key is an immediate parent of this key, False otherwise.
        """
        return self.is_a_parent(parent) and ((self.key_length - 1) == parent.key_length)

    def remove_parent(self, parent_key):
        # type: (Key) -> Key
        """
        Return a new key where a given parent is removed from this key.
        For example, given the key '/my/service/demo' and the parent '/my', the result would be the new key
        '/service/demo'.

        :param parent_key: Parent part to remove from this key.
        :return: A new Key without the given parent.
        :raises KeyException: If this key does not start with parent_key.
        """
        assert isinstance(parent_key, Key), "'parent_key' must be of type 'Key'."
        if not self.is_a_parent(parent_key):
            raise KeyException("Parent key '{}' is not a parent of this key ('{}').".format(parent_key._key, self._key))

        new_key = construct_key(*self._key_parts[parent_key.key_length:])
        return Key.create(new_key)

    def __str__(self):
        # type: () -> str
        """
        :return: The string representation of this key.
        """
        return self._key

    def __repr__(self):
        # type: () -> str
        """
        :return: The string representation of this key.
        """
        return self._key

    def __eq__(self, other):
        # type: (Key) -> bool
        """
        Test that given key is deep-equal to this one.
        :param other: Key to compare to.
        :return: True if a given key is equal to this key, False if not.
        """
        return isinstance(other, Key) and other._key == self._key

    def __starts_with(self, sub_key_parts):
        # type: (List[str]) -> bool
        """
        Test if this key starts with sub_key_parts.

        :param sub_key_parts: Subkey to test.
        :return: True if this key starts with sub_key_parts, otherwise False.
        """
        if len(sub_key_parts) > len(self._key_parts):
            return False

        for index in range(len(sub_key_parts)):
            if sub_key_parts[index] != self._key_parts[index]:
                return False

        return True
