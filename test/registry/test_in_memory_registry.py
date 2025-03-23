import unittest
from collections import OrderedDict

from typing import List, Dict

from scoutlight.registry import KeyDoesNotExist, Key
from scoutlight.registry.in_memory_registry import InMemoryRegistry

SAMPLE_KEY = Key.create("/sampleKey")
SAMPLE_VALUE = "value"


# noinspection DuplicatedCode
class TestInMemoryRegistry(unittest.TestCase):
    """
    A collection ot test cases for testing InMemoryRegistry implementation.
    """

    def __init__(self, *args, **kwargs):
        super(TestInMemoryRegistry, self).__init__(*args, **kwargs)

    def setUp(self):
        """
        Test fixture -- clear all existing data in etcd3 and initialize our registry.
        """

        self.registry = InMemoryRegistry()

    def test_should_get_single_key(self):
        """
        Test that querying for an existing key, we get our expected value.
        """

        self.registry.put(SAMPLE_KEY, SAMPLE_VALUE)

        # Query our registry to look the key up.
        result = self.registry.get(SAMPLE_KEY)

        self.assertEqual(result, SAMPLE_VALUE)

    def test_should_catch_exception_when_key_does_not_exist(self):
        """
        Test that querying for a non-existing key raises an exception.
        """

        # Query for a non-existing key. It should raise KeyDoesNotExist exception.
        self.assertRaises(KeyDoesNotExist, lambda: self.registry.get(SAMPLE_KEY))

    def test_should_list_immediate_children_keys(self):
        """
        Test that the registry return list of all immediate children keys of a given parent.
        """
        self.registry.put("/parent/child1", "")
        self.registry.put("/parent/child2", "")
        self.registry.put("/parent/child3", "")
        self.registry.put("/parent/child3/A", "")
        self.registry.put("/parentA", "")

        result = self.registry.list_keys("/parent")  # type: List[str]

        # Assert we got only the immediate children or '/parent'.
        self.assertListEqual(sorted(result), sorted(["/parent/child1", "/parent/child2", "/parent/child3"]))

    def test_should_list_all_children_keys(self):
        """
        Test that the registry return list of all children keys of a given parent, both immediate and descendant.
        """
        self.registry.put("/parent/child1", "")
        self.registry.put("/parent/child2", "")
        self.registry.put("/parent/child3", "")
        self.registry.put("/parent/child3/A", "")
        self.registry.put("/parentA", "")

        result = self.registry.list_keys("/parent", True)  # type: List[str]

        # Assert we got only the immediate children or '/parent'.
        self.assertListEqual(sorted(result),
                             sorted(["/parent/child1", "/parent/child2", "/parent/child3", "/parent/child3/A"]))

    def test_should_fetch_all_key_value_pairs_for_parent_key(self):
        """
        Test that fetching all keys and values return all the children key/value pairs under a given parent key.
        """
        self.registry.put("/parent/child1", "1")
        self.registry.put("/parent/child2", "2")
        self.registry.put("/parent/child3", "3")
        self.registry.put("/parent/child3/A", "3A")
        self.registry.put("/parentA", "")

        result = self.registry.fetch("/parent", True, True)  # type: Dict[str, str]

        # We expect to get and ordered dictionary with 4 key/value pairs.
        expected_results = OrderedDict([
            ("/parent/child1", "1"),
            ("/parent/child2", "2"),
            ("/parent/child3", "3"),
            ("/parent/child3/A", "3A")
        ])

        self.assertDictEqual(expected_results, result)
