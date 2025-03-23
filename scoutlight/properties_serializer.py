from abc import ABCMeta, abstractmethod
from typing import Any, Optional

from scoutlight.exceptions import DiscoveryException


class PropertiesConverterException(DiscoveryException):
    """
    Top-level exception for all exceptions raised by 'PropertiesConverter'.
    """
    pass


class UnsupportedPropertiesTypeException(PropertiesConverterException):
    """
    Raised when the converter cannot serializer or deserialize data from/to source/target type.
    Typically, when the converter does not support the requested/provided type.
    """
    pass


class MalformedPropertiesException(PropertiesConverterException):
    """
    Raised when input properties (string form) are malformed and could not be deserialized to object.
    """
    pass


class PropertiesConverter(object):
    """
    Serialize and deserializer properties from object domain (e.g.: dictionary, list, custom data objects) to
    raw string and vice versa.
    """
    __metaclass__ = ABCMeta

    def __init__(self, supported_type=None):
        # type: (Optional[type]) -> None
        """
        Class initializer.

        :param supported_type: Type of properties this converter supports. If this value is 'None', then a call to
        'supports' method will always return False.
        """
        self._supported_type = supported_type

    @abstractmethod
    def to_string(self, properties):
        # type: (Any) -> str
        """
        Convert an input properties set to a string representation.
        The input depends on the type supported by the converter.

        :param properties: Properties to be serialized to string form.
        :return: String representation of properties set.
        :raises UnsupportedPropertiesTypeException: If the converted does not support the provided object type.
        """
        raise NotImplementedError()

    @abstractmethod
    def from_string(self, raw_properties):
        # type: (str) -> Any
        """
        Deserialize a raw string into an object.

        The target object is determined by the underling implementation.
        :param raw_properties: Raw string representation of properties to be deserialized.
        :return: Object representation of properties set.
        :raises MalformedPropertiesException: If the raw data could not be deserialized to object.
        """
        raise NotImplementedError()

    def supports(self, obj):
        # type: (Any) -> bool
        """
        Provide indication if the converter supports a given object.

        :param obj: Object to tests.
        :return: True if supported, False otherwise.
        """
        return obj is not None and (self._supported_type is None or isinstance(obj, self._supported_type))

    def _assert_object_type(self, obj):
        # type: (Any) -> None
        """
        Assert that the converter supports the provided object.
        Typically called by underlying implementation from to_str() method.

        :param obj: Object to test.
        :raises UnsupportedPropertiesTypeException: If the converter does not support the provided object type.
        """
        if not self.supports(obj):
            raise UnsupportedPropertiesTypeException(
                "Unsupported object type: {} (this converter supports only objects of type {}).".format(type(obj),
                                                                                                        self._supported_type))

    @staticmethod
    def _assert_input(st):
        # type: (str) -> None
        """
        Assert that a given 'st' type is of type string.
        Typically called from within 'from_string' method.

        :param st: Object to test as a string.
        :raises TypeError: If the provided object is not a string type.
        """
        if not isinstance(st, basestring):
            raise TypeError("Expected a string type, got {}.".format(type(st)))


class JsonDictPropertiesConverter(PropertiesConverter):
    """
    Convert dictionary properties to string representation of JSON and vise versa.
    """

    def __init__(self):
        """
        Class initializer.
        """
        super(JsonDictPropertiesConverter, self).__init__(dict)

    def to_string(self, properties):
        # type: (dict) -> str

        pass

    def from_string(self, raw_properties):
        pass
