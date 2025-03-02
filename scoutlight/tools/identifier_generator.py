import uuid
from abc import ABCMeta


class IdentifierGenerator:
    """
    An abstraction for generating identifiers.
    """

    __metaclass__ = ABCMeta

    def generate(self):
        # type: () -> str
        """
        Generates a unique identifier on each call.

        :return: String identifier.
        """
        raise NotImplementedError()


class UUID4IdentifierGenerator(IdentifierGenerator):
    """
    A simple identifier generator based on Python's UUID4.
    """

    def generate(self):
        # type: () -> str
        """
        :return A UUID4 string.
        """
        return uuid.uuid4().hex
