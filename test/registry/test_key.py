import unittest

from scoutlight.registry.key import Key


class KeyTest(unittest.TestCase):
    """
    Test cases for Key.
    """

    def test_should_create_new_key(self):
        """
        Test that Key.create creates a new key. The key should be normalized.
        """
        key = Key.create("some_key")
        self.assertEqual(str(key), "/some_key")

    def test_should_match_parent_key(self):
        """
        Test that a given key starts with a given parent key.
        """
        key = Key.create("/repository/services/PrintService")

        self.assertTrue(key.is_a_parent(Key.create("repository")))

        # Make sure that a key which is the same as our key cannot be considered as 'parent' (a parent must be
        # shorter than our key).
        self.assertFalse(key.is_a_parent(Key.create("/repository/services/PrintService")))

    def test_should_remove_parent_part(self):
        """
        Test that we can remove a parent part from a key.
        """

        key = Key.create("/repository/services/PrintService")
        parent = Key.create("/repository")

        new_key = key.remove_parent(parent)

        self.assertEqual(new_key, Key.create("/services/PrintService"))

    def test_should_detect_immediate_parent(self):
        """
        Test that a parent key can be identified as an immediate parent.
        """
        key = Key.create("/repository/services/PrintService")
        immediate_parent = Key.create("/repository/services")
        non_immediate_parent = Key.create("/repository")

        self.assertTrue(key.is_immediate_parent(immediate_parent))
        self.assertFalse(key.is_immediate_parent(non_immediate_parent))
