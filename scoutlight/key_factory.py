from scoutlight.tools.key_tools import normalize_key, construct_key


class Keys:

    def __init__(self):
        raise NotImplementedError('Class should not be instantiated.')

    @staticmethod
    def create_cluster_key(cluster_id):
        # type: (str) -> str
        """
        Creates a cluster key for a given cluster id.

        :param cluster_id: Cluster identifier.
        :return: Cluster key.
        """
        assert Keys.__assert_valid_input(cluster_id)
        return normalize_key(cluster_id)

    @staticmethod
    def create_service_base_key(cluster_id):
        # type: (str) -> str
        """
        Creates a key for services' based key (e.g.: /my_cluster/services).

        :param cluster_id: Cluster identifier.
        :return: Service base key.
        """
        return construct_key(Keys.create_cluster_key(cluster_id), 'services')

    @staticmethod
    def create_service_key(cluster_id, service_name):
        # type: (str, str) -> str
        """
        Creates a key for a given service.

        E.g., for cluster 'my_cluster', service 'my_service', creates the key: '/my_cluster/services/my_service'.

        :param cluster_id: Cluster identifier.
        :param service_name: Service name.
        :return: Service key.
        """
        assert Keys.__assert_valid_input(service_name)
        return construct_key(Keys.create_service_base_key(cluster_id), service_name)

    @staticmethod
    def create_service_members_base_key(cluster_id, service_name):
        # type: (str, str) -> str
        """
        Creates service members' base key.

        E.g., for cluster 'my_cluster' and service 'my_service', create the key:
        '/my_cluster/services/my_service/members'.
        """

        return construct_key(Keys.create_service_key(cluster_id, service_name), 'members')

    @staticmethod
    def create_service_instance_key(cluster_id, service_name, instance_id):
        # type: (str, str, str) -> str
        """
        Creates a key for a given service instance.

        :param cluster_id: Cluster identifier.
        :param service_name: Service name.
        :param instance_id: Instance identifier.
        :return: Service instance key.
        """
        assert Keys.__assert_valid_input(instance_id)
        return construct_key(Keys.create_service_members_base_key(cluster_id, service_name), instance_id)

    @staticmethod
    def __assert_valid_input(s):
        # type: (str) -> None
        """
        Assert that a given object is a valid non-empty string.

        :param s: Object to be validated.
        :raises AssertionError: If input is either not a string or an empty string.
        """
        assert isinstance(s, basestring) and len(s.strip()) > 0, "Invalid input."
