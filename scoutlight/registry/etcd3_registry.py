import logging
from collections import OrderedDict
from threading import Thread, Event

import etcd3
from etcd3 import Lease
from etcd3.client import KVMetadata
from etcd3.transactions import Put, Version
from typing import Dict, Optional, List, Tuple, Generator

from scoutlight.registry import Registry, KeyDoesNotExist
from scoutlight.registry.key import Key
from scoutlight.tools.key_tools import normalize_key
from scoutlight.tools.periodic_timer import PeriodicTimer

# Root key where all services are registered under.
ROOT_KEY = Key.create("/registry")

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
        # type: (Etcd3Details, int, Key) -> None
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

        self._root_key = root_key  # type: Key

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

    def _get_one(self, get_key):
        # type: (Key) -> str
        """
        Fetch a single key from the store.

        This should be an optimal operation with as less overhead as possible.

        :param get_key: Key to fetch data for.
        :return: Value associated with the given key.
        :raises KeyDoesNotExist: If the key does not exist.
        """
        etcd3_key = self._to_ectd_key(get_key)

        # all_items = self._client.get_prefix(base_key, sort_order='ascend', sort_target='create')
        value, metadata = self._client.get(etcd3_key)
        if metadata is None:
            raise KeyDoesNotExist("Key does not exist: '{}'.".format(get_key.key))

        return value

    def _get(self, get_key, recursive=False, keep_order=False, keys_only=False, exclude_parent_keys=True):
        # type: (Key, bool, bool, bool, bool) -> Dict[Key, str]

        etcd3_base_key = self._to_ectd_key(get_key) + '/'  # type: str

        if keep_order:
            metadata_list = self._client.get_prefix(etcd3_base_key,
                                                    keys_only=keys_only,
                                                    sort_order='ascend',
                                                    sort_target='create')  # type: Generator[Tuple[str, KVMetadata], None, None]
            results = OrderedDict()
        else:
            metadata_list = self._client.get_prefix(etcd3_base_key,
                                                    keys_only=keys_only)  # type: Generator[Tuple[str, KVMetadata], None, None]
            results = dict()

        for item in metadata_list:
            # Generate key from etcd's response.
            key = self._to_local_key(item[1].key)
            if recursive or (not recursive and key.is_immediate_parent(get_key)):
                results[key.key] = item[0]

        return results

    def _put(self, kv_list, conditional_key_exist=None):
        # type: (List[Tuple[Key, str]], Optional[Key]) -> bool
        """
        Set one or more key(s)/value(s).
        The put operation can be condition, i.e., set only if a given key exists.

        :param kv_list: List of Key/value pair(s).
        :param conditional_key_exist: If defined (non-None), the key/value pairs are set only if the given key does not
                                      exist.
        :return: True if the key/value pairs were set, False otherwise.
        """
        if len(kv_list) > 1 or conditional_key_exist is not None:
            # We are either requested to put multiple values or put one or more value under condition.
            if conditional_key_exist is not None:
                # Condition to put values only if conditional key exists.
                conditional_key = [Version(self._to_ectd_key(conditional_key_exist.key)) > 0]
            else:
                # All values are put without any condition.
                conditional_key = []

            # Construct a list of commands to perform in our transaction.
            put_commands = [Put(self._to_ectd_key(k.key), v, self._lease) for (k, v) in kv_list]

            status, responses = self._client.transaction(conditional_key, [], put_commands)

            # Values were set if the number of responses is greater than 0 (should be 1 response for each value set).
            success = (len(responses) > 0)  # type: bool
        else:
            # We are asked to put only one value. No condition.
            k, v = kv_list[0]
            self._client.put(self._to_ectd_key(k), v, lease=self._lease)

            success = True  # type: bool

        return success

    def _to_local_key(self, key):
        # type: (str) -> Key
        """
        Convert etcd3 key to a local key.

        The operation involves removing a prefix from the key.
        :param key: Key to convert.
        :return: Localized key.
        """
        local_key = Key.create(key)

        if not local_key.is_a_parent(self._root_key):
            raise KeyError("Invalid/unsupported etcd3 key: '{}' (must start with '{}').".format(key, self._root_key))

        return local_key.remove_parent(self._root_key)

    # noinspection SpellCheckingInspection
    def _to_ectd_key(self, key):
        # type: (Key) -> str
        """
        Convert a caller's key to an ECTD key.

        The key is converted for prefixing the key with our local _ROOT_KEY and normalizing it afterward.
        :param key: Key to be converted.
        :return: Compatible internal etcd key.
        """
        return self._root_key.relative(key).key

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
