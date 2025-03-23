from scoutlight.registry import Key


class ClusterKeys(object):
    """
    ClusterKeys object allows easy access to common cluster keys, such as base key, services keys, etc...
    """

    def __init__(self, cluster_id):
        # type: (str) -> None

        assert isinstance(cluster_id, str) and len(cluster_id.strip()) > 0, "Cluster id must be a non-empty string."

        # Raw cluster id.
        self._cluster_id = cluster_id  # type: str

        # Cluster key.
        self._cluster_base_key = Key.create(self._cluster_id)  # type: Key

        # Services' base key.
        self._services_key = self._cluster_base_key.relative('services')  # type: Key

    def cluster_key(self):
        # type: () -> Key
        """
        :return: A key to cluster base key. E.g., for cluster 'my_cluster' this will return the key of '/my_cluster'.
        """
        return self._cluster_base_key

    def services_key(self):
        # type: () -> Key
        """
        :return: Key of cluster's service base key. Typically, '/<cluster_id>/services'.
        """
        return self._services_key

    def service_key(self, service_name):
        # type: (str) -> Key
        """
        Return a key to a cluster service.

        :param service_name: The name of the service.
        :return: Key to the service.
        """
        assert isinstance(service_name, str), "Service name must be a string."
        return self._services_key.relative(service_name)

    def service_instance(self, service_name, instance_id):
        # type (str, str) -> Key
        """
        Return a key to a service instance, typically '/<cluster_id>/services/<service_name>/<instance_id>'.

        :param service_name: The name of the service.
        :param instance_id: Service instance id.
        :return: Key to the service instance.
        """
        return self._services_key.relative(service_name, instance_id)
