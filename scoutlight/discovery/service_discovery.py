import sys

from etcd3 import members
from typing import List, Dict, Optional
from abc import ABCMeta, abstractmethod
from scoutlight.exceptions import DiscoveryException
from scoutlight.key_factory import Keys
from scoutlight.registry import Registry


class ServiceDiscoveryException(DiscoveryException):
    """
    Top level for all service discovery errors.
    """
    pass


class ServiceUnavailableException(ServiceDiscoveryException):
    """
    Indicates that a service referenced either via ServiceDiscovery or ServiceLocator is either does not exist
    or is temporarily unavailable.
    """
    pass


class ServiceLocatorStrategy(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass


class ServiceLocator(object):
    __metaclass__ = ABCMeta

    """
    A locator of service instances within a cluster.

    This implementation encapsulates a lookup strategy that selects the most suitable service instance per each request.

    The default strategy uses round-robin approach, but custom strategies can rely on services' health status (published
    via service instance properties) to select the most suitable service instance available at each given moment.
    """

    def __init__(self, registry, cluster_id, service_name):
        # type: (Registry, str, str) -> None
        """
        Class initializer.

        :param registry: Registry to get a list of available service instances.
        :param cluster_id: Cluster identifier.
        :param service_name: Name of service.
        """
        self._registry = registry  # type: Registry
        self._cluster_id = cluster_id  # type: str
        self._service_name = service_name  # type: str

        self._service_members_key = Keys.create_service_members_base_key(cluster_id, service_name)  # type: str

    @abstractmethod
    def find_service(self):
        # type: () -> ServiceInstance
        """
        Lookup an available service.
        Each underlying service location strategy has its own algorithm to select the most suitable service instance.

        :raises ServiceUnavailableException: If no available services instance found.
        """
        raise NotImplementedError()


class RoundRobingServiceLocator(ServiceLocator):

    def __init__(self, registry, cluster_id, service_name):
        super(RoundRobingServiceLocator, self).__init__(registry, cluster_id, service_name)

        # Maintain a list of known services and their usage statistics.
        self._justice_table = {}  # type: Dict[str, int]

    def find_service(self):
        # type: () -> str
        """
        :return: The identifier of the least used service instance.
        :raises ServiceUnavailableException: If no available services instance found.
        """

        # Fetch the list of all available service instances.
        existing_members = self._registry.list_keys(self._service_members_key)  # type: List[str]

        # Create a new 'justice table' given the available list of instances identifiers.
        # Re-use statistics from existing justice table.
        # During construction - detect the lowest used instance.
        least_used_instance_id = None  # type: Optional[str]
        lowest_frequency = sys.maxint
        new_justice_table = {}  # type: Dict[str, int]
        for member in existing_members:
            usage_frequency = new_justice_table[member] = self._justice_table.get(member, 0)
            if usage_frequency < lowest_frequency:
                lowest_frequency = usage_frequency
                least_used_instance_id = member

        if least_used_instance_id is None:
            raise ServiceUnavailableException("No instance available for service '{}'.".format(self._service_name))

        # Update justice table.
        new_justice_table[least_used_instance_id] = lowest_frequency + 1
        self._justice_table = new_justice_table

        return least_used_instance_id


class ServiceInstance(object):
    """
    Represents a service instance and its properties.
    """

    def __init__(self, cluster_id, service_name, instance_id, properties):
        # type: (str, str, str, Dict[str,str]) -> None
        """
        Class initializer.

        :param cluster_id: The cluster id.
        :param service_name: The service name.
        :param instance_id: Service instance identifier.
        :param properties: Properties of the service instance.
        """
        self.cluster_id = cluster_id  # type: str
        self.service_name = service_name  # type: str
        self.instance_id = instance_id  # type: str
        self.properties = properties  # type: Dict[str, str]


class ServiceDiscovery(object):

    def list_clusters(self):
        # type: () -> List[str]
        """
        List available clusters.
        """
        pass

    def list_services(self, cluster_id):
        # type: (str) -> List[str]
        """
        List available services in a cluster.

        :param cluster_id: Cluster identifier.
        """
        pass

    def list_service_instances(self, cluster_id, service_name):
        #  type: (str, str) -> List[ServiceInstance]
        """
        List available service instances in a cluster.

        :param cluster_id: Cluster identifier.
        :param service_name: Service name.
        :return: List of service instances.
        """
        pass

    def register_service(self, cluster_id, service_name, properties):
        # type: (str, str, Dict[str, str]) -> ServiceInstance
        """
        Register a service in a cluster.
        :param cluster_id: Cluster identifier.
        :param service_name: Service name.
        :param properties: Service properties.
        :return: Service instance object.
        """
        pass

    def unregister_service(self, cluster_id, service_name, instance_id):
        # type: (str, str, str) -> bool
        """
        Unregister a service instance in a cluster.

        :param cluster_id: Cluster identifier.
        :param service_name: Service name.
        :param instance_id: Service instance identifier.
        :return: True if service instance was unregistered, False if it did not exist.
        """
        pass

    def update_service(self, cluster_id, service_name, instance_id, properties):
        # type: (str, str, str, Dict[str, str]) -> None
        """
        Update a service in a cluster.

        :param cluster_id: Cluster identifier.
        :param service_name: Service name.
        :param instance_id: Service instance identifier.
        :param properties: Service properties to set.
        :raises ServiceUnavailableException: If the referenced service/service instance does not exist.
        """
        pass

    def create_service_locator(self, cluster_id, service_name):
        # type: (str, str) -> ServiceLocator
        pass
