import logging
from collections import OrderedDict
from threading import Thread, Event

import etcd3
from etcd3 import Lease
from etcd3.client import KVMetadata
from etcd3.transactions import Put, Version
from typing import Dict, Optional, List, Union, Tuple, Generator

from rolodex.discovery.registry.registry import Registry, KeyDoesNotExist
from rolodex.tools.key_tools import normalize_key, construct_key
from rolodex.tools.periodic_timer import PeriodicTimer

# Root key where all services are registered under.
ROOT_KEY = "/registry"

# Default etcd3 client port.
DEFAULT_ETCD_CLIENT_PORT = 2379

# Default time-to-live for leases. Measured in seconds.
DEFAULT_LEASE_TTL = 12

# Extra time-to-live buffer.
LEASE_TTL_BUFFER = 4

# Default interval, in seconds, for refreshing all leases.
# THIS VALUE MUST BE LESS THAN OR EQUAL TO LEASE TTL.
_DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS = DEFAULT_LEASE_TTL  # type: int

logger = logging.getLogger(__name__)


class Etcd3Details:
    """
    Data class that holds etcd connection details. Required during initialization of Etcd3ServiceDiscoveryStrategy.
    """

    def __init__(self, host, port=DEFAULT_ETCD_CLIENT_PORT):
        # type: (str, int) -> None
        """
        Class initialization.

        :param host: Hostname or IP address of the etcd server to connect to.
        :param port: The etcd port to connect to. Defaults to 2379.
        """

        assert isinstance(host, str) and len(host.strip()), "Invalid host."
        assert isinstance(port, int) and port > 0, "Invalid port."

        self.host = host  # type: str
        self.port = port  # type: int


# Etcd details object for local host (useful for development).
ETCD_LOCALHOST = Etcd3Details("localhost")

# The maximum number of attempts to make when trying to create a service and instance identifier collision occurs.
MAX_SERVICE_CREATION_RETRY_COUNT = 10


class _Etcd3KeyValueRecord:
    """
    Represents a key and value retrieved from etcd service.
    """

    def __init__(self, key, key_parts, key_name, value):
        # type: (str, str,str, str) -> None
        """
        Class initializer.

        The 'key_parts' represents the parts of the key, e.g., '/_etcd/registry/<cluster_id>/services/<service_name>'
        represented as ['_etcd', 'registry', '<cluster_id>', 'services', '<service_name>'].

        :param key: The complete key as a single string.
        :param key_parts: Key parts, e.g., for '/parent/childA/X' this parameter would be ['parent', 'childA', 'X']
        :param key_name: The final part of the key, e.g., for ''/parent/childA', this parameter would be 'childA'.
        :param value: Value.
        """
        self.key = key
        self.key_parts = key_parts
        self.key_name = key_name
        self.value = value


class Etcd3Registry(Registry):

    def __init__(self, details, lease_ttl=DEFAULT_LEASE_TTL, root_key=ROOT_KEY):
        # type: (Etcd3Details, int, str) -> None
        """
        Class initialization.

        :param details: Etcd3 connection properties.
        :param lease_ttl: Lease time to live, in seconds. Default to 12 seconds.
        :param root_key: Base key to use for all etcd3 interactions. All keys will begin with this key.
        """
        super(Etcd3Registry, self).__init__()

        assert isinstance(details,
                          Etcd3Details), "'details' parameter must be an instance of Etcd3Details."

        assert isinstance(lease_ttl, int) and lease_ttl > 1, "'lease_ttl' must be an integer greater than 1."

        # Flag indicated if this instance is fully initialized and ready for use or not.
        self._initialized = False  # type: bool
        self._thread = None  # type: Optional[Thread]
        self._event = None  # type: Optional[Event]

        self._client = etcd3.client(host=details.host, port=details.port)
        self._lease_ttl = lease_ttl  # type: int

        # Our lease that keeps all our keys alive.
        self._lease = None  # type: Optional[Lease]

        self._lease_periodic_refresh_timer = None  # type: Optional[PeriodicTimer]

        self._root_key = root_key  # type: str

    def setup(self):
        """
        Register the cluster within etcd if not already registered and initialize periodic lease refresh timer.
        """
        self._initialized = False

        # Create a new lease that will serve all our registered keys.
        self._lease = self._client.lease(self._lease_ttl + LEASE_TTL_BUFFER)

        # Periodic timer to refresh our lease.
        self._lease_periodic_refresh_timer = PeriodicTimer(_DEFAULT_LEASE_REFRESH_INTERVAL_SECONDS,
                                                           self.__periodic_lease_refresh_handler)
        self._lease_periodic_refresh_timer.start()
        self._initialized = True

    def destroy(self):
        """
        A tear-down call. When issued, the service removes all locally registered services, releases all resources and
        shuts down.
        To re-use the service, a user must issue a call to 'setup()'.

        """
        # Revoke our lease so all registered keep will expire.
        if self._lease is not None:
            lease = self._lease
            self._lease = None
            lease.revoke()

        # Stop periodic refresh timer.
        self._lease_periodic_refresh_timer.stop()
        self._lease_periodic_refresh_timer = None

        self._initialized = False

    def put_if_not_exist(self, key, value):
        # type: (str, str) -> bool
        self.__assert_key(key)
        self.__assert_value(value)
        self.__assert_state()

        key = self._to_ectd_key(key)

        # Construct a list of commands to perform in our transaction.
        status, responses = self._client.transaction([Version(key) > 0], [], [Put(key, value, lease=self._lease)])

        return len(responses) > 0

    def put(self, key, value):
        # type: (str, str) -> None
        self.__assert_key(key)
        self.__assert_value(value)
        self.__assert_state()

        key = self._to_ectd_key(key)
        self._client.put(key, value, lease=self._lease)

    def put_all(self, values):
        # type: (Union[Dict[str, Optional[str]], Tuple[str, Optional[str]], List[Tuple[str, Optional[str]]]]) -> None

        self.__assert_state()

        put_values = []  # type: List[Put]

        def append_pair(k, v):
            self.__assert_key(k)
            self.__assert_value(v)
            put_values.append(Put(self._to_ectd_key(k), v))

        if isinstance(values, dict):
            for key, value in values.items():
                append_pair(key, value)
        elif isinstance(values, tuple):
            assert len(values) == 2, "Invalid tuple length (must be 2 -- key/value pair."
            append_pair(values[0], values[1])
        elif isinstance(values, list):
            for list_item in values:
                assert isinstance(list_item, tuple) and len(
                    list_item) == 2, "Values list contains a non-tuple key/value pair."
                append_pair(values[0], values[1])

        # Construct a list of commands to perform in our transaction.
        status, responses = self._client.transaction([], put_values, [], lease=self._lease)
        if len(responses) != len(put_values):
            logger.error("Failed to set values.")

    def get(self, key):
        # type: (str) -> str

        self.__assert_key(key)
        self.__assert_state()

        etcd3_key = self._to_ectd_key(key)

        # all_items = self._client.get_prefix(base_key, sort_order='ascend', sort_target='create')
        value, metadata = self._client.get(etcd3_key)
        if metadata is None:
            raise KeyDoesNotExist("Key does not exist: '{}'.".format(key))

        return value

    def list_keys(self, parent_key, recursive=False, keep_order=False):
        # type: (str, bool, bool) -> List[str]

        records = self._query(parent_key, recursive, keep_order)  # type: List[_Etcd3KeyValueRecord]
        return [r.key for r in records]

    def fetch(self, parent_key, recursive=False, keep_order=False):
        # type: (str, bool, bool) -> Dict[str, str]

        records = self._query(parent_key, recursive, keep_order)  # type: List[_Etcd3KeyValueRecord]

        if keep_order:
            result_dict = OrderedDict((r.key, r.value) for r in records)
        else:
            result_dict = {r.key: r.value for r in records}

        return result_dict

    def _to_ectd_key(self, key):
        # type: (str) -> str
        """
        Convert a caller's key to an ECTD key.

        The key is converted for prefixing the key with our local _ROOT_KEY and normalizing it afterward.
        :param key: Key to be converted.
        :return: Compatible internal etcd key.
        """
        return normalize_key(construct_key(self._root_key, key))

    def _to_local_key(self, key):
        # type: (str) -> str
        """
        Convert etcd3 key to a local key.

        The operation involves removing a prefix from the key.
        :param key: Key to convert.
        :return: Localized key.
        """
        if not key.startswith(self._root_key):
            raise KeyError("Invalid/unsupported etcd3 key: '{}' (must start with '{}').".format(key, self._root_key))

        return key[len(self._root_key):]

    def _query(self, base_key, recursive=False, sort_by_revision=False):
        # type: (str, bool, bool) -> List[_Etcd3KeyValueRecord]
        """
        Query for all keys and values under a given base key.
        :param base_key:  Base key to query data for.
        :param recursive: True if to bring all children of given base key, False if to return only immediate children.
        :param sort_by_revision: True if the returned list should be sorted by creation order, False if not sorting
                                 required.
        :return: List of _Etcd3KeyValueRecord(s) representing all keys and values under a given base key.
        """

        self.__assert_key(base_key)
        self.__assert_state()

        etcd3_base_key = self._to_ectd_key(base_key)  # type: str

        # Append forward slash so we'll be looking for children of the given key only.
        etcd3_base_key += '/'

        if sort_by_revision:
            metadata_list = self._client.get_prefix(etcd3_base_key,
                                                    sort_order='ascend',
                                                    sort_target='create')  # type: Generator[Tuple[str, KVMetadata], None, None]
        else:
            metadata_list = self._client.get_prefix(
                etcd3_base_key)  # type: Generator[Tuple[str, KVMetadata], None, None]

        # Convert etcd3 Python library to local records.
        records = self.__to_local_records(metadata_list)  # type: List[_Etcd3KeyValueRecord]

        # In case we do not want to get all keys but rather only the immediate children -- filter out all
        # non-relevant records.
        if not recursive:
            required_key_size = len(normalize_key(base_key).split('/')) + 1
            records = [r for r in records if len(r.key_parts) == required_key_size]

        return records

    def __assert_state(self):
        # type: () -> None
        """
        Test that this instance is set up and ready to be used.
       """
        assert self._initialized, "Etcd3 registry is not initialized. Please call setup() first."

    @staticmethod
    def __assert_key(key):
        # type: (str) -> None
        """
        Assert that a given key is valid (non-empty string).

        :param key: Key to examine.
        :raises AssertionError: If key is not valid.
        """
        assert isinstance(key, str) and len(key.strip()) > 0, "Invalid key (must be non-empty string)."

    @staticmethod
    def __assert_value(key):
        # type: (str) -> None
        """
        Assert that a given value is valid (string type).

        :param key: Key to examine.
        :raises AssertionError: If value is not valid.
        """
        assert isinstance(key, basestring), "Invalid value (must be a string type, got {} instead.).".format(type(key))

    def __to_local_records(self, items):
        # type: (List[Tuple[str, KVMetadata]]) -> List[_Etcd3KeyValueRecord]
        """
        Translate an etcd3 library items to local records.

        :param items: List of items to translate.
        :return: List of _Etcd3KeyValueRecord instance.
        """
        records = []  # type: List[_Etcd3KeyValueRecord]
        for item in items:
            key = self._to_local_key(item[1].key)
            key_parts = key.split('/')
            value = item[0]
            records.append(_Etcd3KeyValueRecord(key, key_parts, key_parts[-1], value))

        return records

    def __periodic_lease_refresh_handler(self):
        # type: () -> None
        """
        Refresh our lease periodically.
        """
        if self._lease is not None:
            # noinspection PyBroadException
            try:
                self._lease.refresh()
            except:
                logger.exception("Failed to refresh lease {}".format(self._lease.id))
