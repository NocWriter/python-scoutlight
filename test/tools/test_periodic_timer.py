import time
import unittest

from scoutlight.tools.periodic_timer import PeriodicTimer


class Counter:

    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1


class TestPeriodicTimer(unittest.TestCase):

    def test_should_start_and_stop_timer(self):
        """
        Test that the periodic timer starts and stops correctly.
        """

        timer = PeriodicTimer(1, lambda _: None)
        timer.start()
        self.assertTrue(timer.is_running())
        timer.stop()
        self.assertFalse(timer.is_running())

    def test_should_issue_callbacks(self):
        """
        Test that our time issue calls to the callback function as predicted.
        """
        counter = Counter()

        timer = PeriodicTimer(1, counter.increment)
        timer.start()
        time.sleep(3)
        timer.stop()

        # The periodic timer was running for 3 seconds with internal of 1 second.
        # Callback should have been issued between 2 and 4 times.
        self.assertTrue(2 <= counter.value < 4)

    def test_should_fail_timer_thread_due_to_errors(self):
        """
        Test should cause the periodic time to terminate after a few consecutive failures.
        """

        def raise_exception():
            raise Exception()

        timer = PeriodicTimer(0.5, raise_exception)
        timer.set_max_failure_count(3)
        timer.start()
        time.sleep(2)

        # Make sure our timer has exited after 2 failures.
        self.assertFalse(timer.is_running())
        self.assertEqual(timer.failure_count, 3)
