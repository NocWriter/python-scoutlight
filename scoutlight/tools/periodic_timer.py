import logging
from threading import Thread, Event, current_thread
from time import time

from typing import Callable, Union

logger = logging.getLogger(__name__)

# Default maximum consecutive failure counts before the timer exists.
DEFAULT_MAX_FAILURE_COUNT = 10


class PeriodicTimer:
    """
    A periodic timer that issues a call to predefine callback in predefined internal.

    The periodic timer supports the notion of "failure counts exit", which will cause the timer to exit when after
    a certain number of consecutive failures occurred.
    """

    def __init__(self, interval, callback, name=None):
        # type: (Union[int, float], Callable, str) -> None
        """
        Class initializer.

        :param interval: The interval in seconds.
        :param callback: A callback function to call periodically.
        :param name: Optional name for the periodic timer thread.
        """
        assert isinstance(interval, (int, float)), "interval must be numeric value."
        assert interval > 0, "interval must be positive."
        assert callable(callback), "invalid callback parameter."

        self._interval = interval  # type: int
        self.callback = callback  # type: Callable

        self._thread = Thread(target=self._run, name=name)
        self._thread.setDaemon(True)
        self._stop_event = Event()

        # Measures the number of consecutive failures.
        self._failure_count = 0  # type: int
        self._max_failure_count = DEFAULT_MAX_FAILURE_COUNT

        # Flag indicating if the periodic thread currently running or not.
        self._running = False  # type: bool

    def start(self):
        # type: () -> None
        """
        Starts the periodic timer.
        """
        assert not self._running, "Periodic timer already running."
        assert not self._stop_event.is_set(), "Periodic timer terminated; cannot restart."

        self._thread.start()

    def stop(self):
        # type: () -> None
        """
        Stops the periodic timer and wait for the thread to exit.
        """
        if self._thread.is_alive():
            self._stop_event.set()
            self._thread.join()

    def is_running(self):
        # type: () -> bool
        """
        :return: True if thread is running, False otherwise.
        """
        return self._running

    def set_max_failure_count(self, max_failure_count):
        # type: (int) -> None
        """
        Sets the maximum consecutive number of failures after which the timer terminates. Set a negative value to
        disable.

        :param max_failure_count:
        :return:
        """
        assert isinstance(max_failure_count, int), "max_failure_count must be an integer."
        self._max_failure_count = max_failure_count

    @property
    def failure_count(self):
        # type: () -> int
        """
        :return: The number of consecutive failure counts. If the last cycle was successful, this value is reset
        back to zero.
        """
        return self._failure_count

    def _run(self):
        """
        A thread function that runs in loop periodically issues the callback.
        """

        self._running = True
        thread_name = current_thread().name

        start_time = time()  # type: float

        # Run as long as our event has not been set.
        while not self._stop_event.wait(self._interval):
            # noinspection PyBroadException
            try:
                self.callback()
                self._failure_count = 0
                start_time = time()
            except Exception:
                # Upon failure -- log the error.
                logger.exception("Error during periodic timer callback.")

                # If timer supports termination after consecutive failures - terminate thread.
                self._failure_count += 1
                if 0 < self._max_failure_count <= self._failure_count:
                    elapsed_time = time() - start_time  # type: float

                    logger.error(
                        "Maximum number of failures reached (count: {}) in {:.2f} seconds. "
                        "Terminating periodic timer thread ({})."
                        .format(thread_name, elapsed_time, self._failure_count))
                    self._stop_event.set()

        logger.info("Periodic timer thread terminated ({}).".format(thread_name))
        self._running = False
