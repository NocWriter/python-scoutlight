import inspect

from typing import List, Any


class SequenceMismatchError(Exception):
    """
    This exception is raised when a string item cannot be processed.
    """

    def __init__(self, message):
        # type: (str) -> None
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message


class SequenceMatcher(object):
    """
    A simple callback that can match a set of strings.
    """

    def __init__(self, *args):
        # Holds a list of strings to match. We expect 'args' to contain only strings.
        self._args = [st for st in args]  # type: List[str]
        self._index = 0

    def __call__(self, index, target, value):
        """
        Validate that a given value matches an expected string within a predefined list.
        :param index:
        :param target:
        :param value:
        :return:
        """
        if self._args[self._index] != value:
            raise SequenceMismatchError(
                "Unexpected value '{}' at offset {} (expected '{}').".format(value, index, self._args[index]))
        self._index += 1
        return index == len(self._args)

    def reset(self):
        # type: () -> None
        """
        Called by the framework before start processing any sequence of string.

        Reset the instance to its initial state.
        """
        self._index = 0


class StringSequenceProcessor(object):
    """
    A chain of responsibilities implementation for processing a sequence of strings.
    """

    def __init__(self, target_type):
        # type: (type) -> None
        """
        Class initializer.
        """
        assert (isinstance(target_type, (dict, list, tuple)) or
                inspect.isclass(target_type)), \
            "Parameter 'target_type' must be an object type."

        self._target_type = target_type

        # List of processors to apply on each part of the input data items.
        self._handlers = []  # type: callable

    def add_handler(self, callback):
        # type: (callable) -> None
        """
        Add a callback to the chain.

        :param callback: Callback to add. The callable is expected to accept three parameters:
                         (index, target object, value) and return either None or boolean value.
                         Either a boolean value of True or None value indicates the processor framework will proceed to
                         the next handler. A value of False will indicate the processing framework to call this handler
                         next time again.
        """
        assert callable(callback), "Parameter must be a valid callable."
        self._handlers.append(callback)

    def add_exact_match_handler(self, *args):
        # type: (str) -> None
        """
        Add a handler that performs an exact match between a given text and a data item.
        :param args: List of arguments to expect.
        :raises ValueError: If the text does not match given data item.l
        """
        assert len(args) > 0, "Expected at least 1 argument."
        self.add_handler(SequenceMatcher(*args))

    def execute(self, target, data_items):
        # type: (Any, List[Any]) -> None
        """
        Execute the chain of responsibilities for a given items list.

        :param target: Target model to store data into.
        :param data_items: List of items to execute on.
        """

        assert isinstance(target,
                          self._target_type), "Target model is not of an expected type (expected: {}, got: {}).".format(
            self._target_type, type(target))

        # For handlers that have 'reset' callback -- issue it before execution.
        for handler in self._handlers:
            if hasattr(handler, 'reset'):
                handler.reset()

        data_items_count = len(data_items)
        handlers_count = len(self._handlers)

        data_index = 0
        handler_index = 0
        while data_index < data_items_count and handler_index < handlers_count:
            handler = self._handlers[handler_index]
            result = handler(data_index, target, data_items[data_index])
            data_index += 1

            if result is None or result == True:
                handler_index += 1

        # If not all items processed, raise exception.
        if data_index < len(data_items):
            raise ValueError("Not all items processed ({} items processed out of {} items)."
                             .format(data_index, data_items_count))
