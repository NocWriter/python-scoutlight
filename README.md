ScoutLight - Service discovery made easy
========================================

Preface
-------
ScoutLight is a client-side service-discovery library
intended to be easy-to-use yet powerful and configurable enough for your any needs.

The library attempts to follow [The twelve factor app](https://12factor.net/) and the
[SOLID design principles](https://www.freecodecamp.org/news/solid-design-principles-in-software-development/).

At core, the library requires three mandatory properties: __cluster id__ (or cluster name),
a __service name__ and __service instance identifier__.

Background
----------
Imaging is a micro-service-based architecture application.
Each service stands by its own.
Each service may have a number of running instances and each instance may have its own set
of protocols it supports (e.g.: HTTPS, AMQP, gRPC, ...) and other qualitative properties.

Architecture
------------
The library is composed of four parts, each can be either used directly, replaced or dropped out:

* [Registry](#registry)
* Schema manager
* Service discovery
* HTTP client


        +----------------------------------------+
        |               HTTP client              |
        +----------------------------------------+
        |            Service discovery           |
        +----------------------------------------+
        |             Schema manager             |
        +----------------------------------------+
        |                Registry                |
        +----------------------------------------+

### <a name="registry"></a>registry
------------
The registry, at its core, is a simple hierarchical key/value store allowing any type of
string-to-string mapping.
Much like a file-system, it has a parent / child relationship between keys.
For example, having a key such as `/MyCluster/services/PrintingServices/4623452345/hostname`,
The part `MyCluster` is the parent of `services`, which in turn is the parent of `PrintingServices`
and so forth.

The registry does not restrict any specific registration form or limitations.
You can write any value you see fit.
The current implementation supports etcd3 (via etcd3-python) library and in-memory (transient) registry.
The latter is good for unit-testing.
Plans expect to support other storage services such as Redis, Consul and even PostgresQL.

You can access the registry directly and use it for your own needs as well, but try not to
collide with the namespaces reserved for the service discovery.

Schema manager
--------------
The schema manager is the actual power behind the registry.
It serializes and deserializes service information from/to the registry.

The schema manager always supports three key properties: __cluster id__, __service name__, and
__service instance identifier__.

* Cluster id: Groups all services and other properties under one logical unit.
  Typically, each application has one cluster describing all its services.
  A service registry can support multiple applications via multiple clusters.
* Service name: Identifies the service we want to locate, e.g.: _PrintingService_, _UserManagementService_,
  _Authenticator_.
* Service instance id: Since each service may have more than one instance, a unique identifier identifies each service
  instance.

Typically, a schema manager will map a structure of cluster id, service name and service instance id into a base key
of the following form:

    <cluster_id>/service/<service_name>/<service_instance_id>

Although this structure is enough to reference a service and its instances in a cluster, a schema manager will typically
include one or more properties per service instance, e.g.:

    <cluster_id>/service/<service_name>/<service_instance_id>/<property_name>

For example, running on a cluster named _clusterA_ with a service named _Printer_, where each
service has properties such as hostname, port number and protocol type (e.g.: HTTP/JMS/AMQP):

    Cluster ID: clusterA
    Service name: Printer
    Service instance: 640da76bb2e3
    Hostname: 192.168.1.101
    Port: 8001
    protocol: https

    Cluster ID: clusterA
    Service name: Printer
    Service instance: d769c3aafdd4
    Hostname: 192.168.1.220
    Port: 8080
    protocol: amqp

The schema manager may choose to map it into a key/value pair as:

       
| Key                                               | Values        |
|---------------------------------------------------|---------------|
| /clusterA/services/Printer/640da76bb2e3/hostname  | 192.168.1.101 |
| /clusterA/services/Printer/640da76bb2e3/port      | 8001          |
| /clusterA/services/Printer/640da76bb2e3/protocol  | https         |
| /clusterA/services/Printer/d769c3aafdd4/hostname  | 192.168.1.220 |
| /clusterA/services/Printer/d769c3aafdd4/port      | 8080          |
| /clusterA/services/Printer/d769c3aafdd4/protocol  | amqp          |
    
In such a way, the schema manager abstracts the translation details from/to the registry, allowing
a caller to work with value objects (data objects) instead of key/value primitives.

The schema manager is a generic serializer/deserializer.
It is not bound to a specific form or key/value patterns.
However, the schema manager comes with pre-build serializers for ease of use.

Service discovery
-----------------
The service discovery is the beating heart of the library.
It uses the schema manager and resolution strategy to locate a service.
It hides the little tedious details from the user.

Via the schema manager, the service discovery finds all matching services that the resolution strategy
selects a single service instance.
During configuration time of the discovery service, a caller can decide which strategy to use.
For example, the user can apply a built-in round-robin strategy (each time a different service instance
is returned to the caller).

A more complex resolution strategy may choose to select each time the more reliable and less occupied service instance.
Since the Schema manager can maintain any number of properties, it may also provide health and quality-of-service
properties.
A resolution strategy may choose to consult such properties to decide which service instance is the most suitable for
use at any given time.

HTTP client.
---------------------
The final piece in this toolbelt is the HTTP client. 
While it is less frequently used compared to other building blocks, this helpful tool can ease the execution of
REST-based API calls.

The HTTP client can accept a service name and payload.
It runs all the tedious work to locate (via the Discovery Service) an available service and execute the call.

It is a most configurable HTTP client implementation that can both be pre-configured or can be
overridden to extend its behavior.
