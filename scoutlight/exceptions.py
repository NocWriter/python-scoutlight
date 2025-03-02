class DiscoveryException(Exception):

    def __init__(self, message):
        # type: (str) -> None
        """
        Class initializer.

        :param message: Error message.l
        """
        self.message = message
