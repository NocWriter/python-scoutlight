import inspect
from abc import ABCMeta, abstractmethod

from typing import Any, List

from scoutlight.exceptions import DiscoveryException


class ObjectEditorException(DiscoveryException):
    """
    Base exception thrown by object-editor classes.
    """
    pass


class ObjectEditor(object):
    __metaclass__ = ABCMeta

    """
    An object editor allows a caller to manipulate an object's state (data) via programmatic API, without knowing
    the exact details of the object.
    """

    def __init__(self):
        pass

    @abstractmethod
    def set_value(self, obj, key, value):
        # type: (Any, str, Any) -> None
        """
        Set an object's property or attribute. Property/attribute denoted by 'key' and its value is set by 'value'.

        :param obj: Object to be set property or attribute on.
        :param key: Attribute/property to be set.
        :param value: Value to assign.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_value(self, obj, key):
        # type: (Any, str) -> Any
        """
        Retrieve a key value from the object.

        :param obj: Object to be retrieved property or attribute from.
        :param key: Attribute/property to retrieve.
        :raises AttributeError: If attribute/property does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def supports(self, obj):
        # type: (Any) -> bool
        """
        Test if this editor supports a given object type.
        """
        raise NotImplementedError()

    @staticmethod
    def _assert_key(key):
        # type: (str) -> None
        """
        Assert that a given key is a valid string.
        """
        assert isinstance(key, basestring), "Key must be a string."

    def _assert_support(self, obj):
        # type: (Any) -> None
        """
        Test that this editor supports a given object type. If not, an exception is raised.

        :param obj: Object to be tested.
        :raises AssertionError: If given object is not supported.
        """
        assert self.supports(obj), "Object of type {} is not supported by this editor.".format(obj.__class__.__name__)

    def _assert_object_and_key(self, obj, key):
        # type: (Any, str) -> None
        """
        Assert that this editor supports the given object and the key is a valid string.

        Same as calling self._assert_key(key) and self._assert_support(obj).
        """
        self._assert_key(key)
        self._assert_support(obj)


class DictObjectEditor(ObjectEditor):

    def set_value(self, obj, key, value):
        # type: (Any, str, Any) -> None
        self._assert_object_and_key(obj, key)
        obj[key] = value

    def get_value(self, obj, key):
        # type: (Any, str) -> Any
        self._assert_object_and_key(obj, key)
        try:
            return obj.get(key)
        except KeyError:
            raise ObjectEditorException("Attribute/property does not exist: '{}'.".format(key))

    def supports(self, obj):
        # type: (Any) -> bool
        """
        Test that the given object is a dictionary type.
        """
        return isinstance(obj, dict)


class ClassObjectEditor(ObjectEditor):

    def set_value(self, obj, key, value):
        # type: (Any, str, Any) -> None
        self._assert_object_and_key(obj, key)
        if hasattr(obj, key) and inspect.ismethod(getattr(obj, key)):
            raise ObjectEditorException(
                "Attribute/property '{}' is a method. Cannot write over it with simple value..".format(key))

        setattr(obj, key, value)

    def get_value(self, obj, key):
        # type: (Any, str) -> Any
        self._assert_object_and_key(obj, key)
        try:
            return getattr(obj, key)
        except AttributeError:
            raise ObjectEditorException("Attribute/property does not exist: '{}'.".format(key))

    def supports(self, obj):
        # type: (Any) -> bool
        """
        Test if the given object is a user-defined class.
        """
        return obj.__class__.__module__ not in ["builtins", "__builtins__"]


class ObjectEditorRegistry(object):
    """
    A registry that aggregates multiple editors.

    The registry allows a caller to perform standard get/set operations on a given object.
    It will look for an object editor that can handle the object and apply operations on it.
    """

    def __init__(self):
        # Maintains a list of object editors.
        self._registry = []  # type: List[ObjectEditor]

    def add_editor(self, editor):
        # type: (ObjectEditor) -> None
        """
        Register a new editor in this registry.

        :param editor: Editor to add.
        """
        self._registry.append(editor)

    def find_editor_for(self, obj):
        # type: (Any) -> ObjectEditor

        for editor in self._registry:
            if editor.supports(obj):
                return editor

        raise ObjectEditorException("No editor found for object of type {}".format(type(obj)))

    def get_value(self, obj, key):
        # type: (Any, str) -> None
        """
        Lookup an object editor that can handle the given object and apply 'get_value' to it.

        :param obj: Object to get value from.
        :param key: Attribute/property to fetch.
        :return: Value from the given object.
        :raises AttributeError: If attribute/property does not exist.
        :raises ObjectEditorException: If no editor found for the given object.
        """
        self.find_editor_for(obj).get_value(obj, key)

    def set_value(self, obj, key, value):
        # type: (Any, str, Any) -> None
        """
        Lookup an object editor that can handle the given object and apply 'get_value' to it.

        :param obj: Object to set value on.
        :param key: Attribute/property to set.
        :param value: Value to set on the given object.
        :raises AttributeError: If attribute/property is a method and cannot be overridden with a given value.
        :raises ObjectEditorException: If no editor found for the given object.
        """
        self.find_editor_for(obj).set_value(obj, key, value)


# An object editor registry with basic object editors.
object_editor_registry = ObjectEditorRegistry()
object_editor_registry.add_editor(DictObjectEditor())
object_editor_registry.add_editor(ClassObjectEditor())
