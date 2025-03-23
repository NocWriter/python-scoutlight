class DiscoveryException(Exception):

    def __init__(self, message):
        # type: (str) -> None
        """
        Class initializer.

        :param message: Error message.l
        """
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message