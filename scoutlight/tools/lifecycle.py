class LifecycleException(Exception):
    pass


class Lifecycle(object):
    # State indicating that the object was created (after call to __init__) but was not setup yet.
    CREATED = "CREATED"

    # State indicating that a call to 'setup' was made and the instance is ready for use.
    INITIALIZED = "INITIALIZED"

    # State indicating that a call to 'destroy' was made. The instance cannot be reused.
    DESTROYED = "DESTROYED"

    def __init__(self):
        # type: () -> None
        """
        Class initializer.
        """
        self.__lifecycle_state = Lifecycle.CREATED

    def setup(self):
        # type: () -> None
        if self.__lifecycle_state == Lifecycle.CREATED:
            # That's fine -- we're switching state to 'INITIALIZED'.
            self.__lifecycle_state = Lifecycle.INITIALIZED
        elif self.__lifecycle_state == Lifecycle.INITIALIZED:
            raise LifecycleException("{} is already initialized.".format(self.__class__.__name__))
        else:
            # DESTROYED
            raise LifecycleException(
                "{} has been destroyed and cannot be re-initialized.".format(self.__class__.__name__))

    def destroy(self):
        # type: () -> None
        """
        Mark this lifecycle as destroyed.
        """
        self.__lifecycle_state = Lifecycle.DESTROYED

    @property
    def lifecycle_state(self):
        # type: () -> str
        """
        Returns the lifecycle state of this object.
        """
        return self.__lifecycle_state

    def _assert_state(self):
        # type: () -> None
        """
        Test if this object is in initialized state or not. If it is not, an exception is raised to indicate
        the object cannot be used.
        """
        if self.lifecycle_state == Lifecycle.CREATED:
            raise LifecycleException("{} has not been initialized yet.".format(self.__class__.__name__))
        elif self.lifecycle_state == Lifecycle.DESTROYED:
            raise LifecycleException(
                "{} has been destroyed and cannot be used anymore.".format(self.__class__.__name__))
