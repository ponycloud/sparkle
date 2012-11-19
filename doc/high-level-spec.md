% High Level Specification

# Project Features

 *  Installation instructions fit on a single page.

## Networking

 *  Traditional flat networks without separation.
 *  Arbitrary number of virtual networks private to tenants,
    some shared between multiple tenants as needed.

## Storage

 *  Both provides and consumes a distributed, highly available storage.
 *  Client to traditional SAN storage with ability to further divide
    individual LUNs.
 *  Able to connect storage volumes to multiple instances, where supported by
    the underlying storage
 *  Able to use local storage in development and testing setups.

## Placement

 *  Automatic instance placement that keeps both cloud-wide load leveling and
    rare resources such as fibre channel in mind.
 *  Placement restrictions that allow for safe virtual clusters.

## High Availability

 *  Architecture allows for both dedicated, clustered controller for large
    setups or a floating controller resilient to split-brain situations for
    smaller ones.
 *  Host outage tolerance by allocating configurable amount of reserve space
    that is used to host instances of failed hosts.

# Internal Components

## Rainbow

Provides a web dashboard for both operators and users to comfortably manage
and utilize the distributed cloud infrastructure.  It is the primary client
of the Sparkle service.

### Features

 *  Can connect to multiple Sparkle instances at the same time.
 *  Full support of all functionality provided by the Spakle RESTful API.
    *  Manages desired state of hosts, instances, volumes, and networks.
    *  Relays current state of all managed resources in a friedly way.
    *  Manages virtual image library.
    *  Manages user accounts, tenants and access control.

## Luna

Manages provisioning of physical machines.  It is basically a high-level
dnsmasq interface combined with fencing agent.

### Features

 *  Network-boots stateless Twilight nodes from current image version.
 *  Performs fencing on Sparkle's behalf, notifying it when sucessfull.

## Sparkle

Holds the central database with configuration of physical machines, networks,
storage, virtual machines and identities.

### Features

 *  Manages desired state of all aspects of the environment.
 *  Relays desired state Twilight nodes and collects information about
    current state and event notifications.
 *  Provides public RESTful API to the full functionality of the platform.
 *  Performs authentication, authorization and accounting.
 *  Computes instance placement, controls host evacuation, fencing and
    relocation of instances during host outage and takes care of other
    aspects of central orchestration.
 *  During normal operation keeps all persistent data safe in a database.

## Twilight

Twilight is the agent that runs on the physical nodes.  It performs all the
organization magic required to convince `libvirt`, storage and networking to
behave as specified by Sparkle.

### Features

 *  Discovers hardware resources.
 *  Manages host's resources, such as both physical and virtual networks,
    local and shared physical storage, virtual storage volumes and instances.
 *  Performs no action on it's own, always merely applicates desired state.
 *  Holds a copy of bootstrap configuration that is used during controller
    startup.
 *  Provides `cloud-init` service to instances it hosts.
 *  Informs Sparkle about interesting conditions using notifications.
 *  Collects performance data and sends them to a central service.

## Support Services

Some services required for the cloud can be hosted externally, or as so-called
service instances, that are treated specially.  These can be, for example:

 *  Persistent [PostgreSQL][] database.
 *  Central [collectd][] service.
 *  Up to five [ZooKeeper][] instances.
 *  VPN gateway.

# External Components

## [KVM][]

Full-featured hypervisor for Intel and ARM. Lately backed up by Red Hat
and other large vendors from the [Open Virtualization Alliance][].

## [libvirt][]

A toolkit to interact with several hypervisors as well as with virtual
networks and various types of storage. Integrates perfectly with KVM.

## [Sheepdog][]

Distributed storage system for QEMU/KVM. It provides highly available
block level storage volumes that can be attached to QEMU/KVM virtual
machines. Sheepdog scales to several hundreds nodes, and supports
advanced volume management features such as snapshot, cloning, thin
provisioning and differential backups.

KVM can connect to sheepdog directly and use it as virtual storage.

## [ZeroMQ][]

Performant and scalable distributed asynchronous messaging library.

## [dnsmasq][]

Lightweight but flexible network boot server.

## [Twisted][]

Python framework for asynchronous network-related programming.

## [psycopg2][]

Set of python binding for connecting to PostgreSQL database with asynchronous
query support that can integrate with Twisted.

## [Werkzeug][]

Python WSGI framework.

## [Flask][]

Python microframework based on Werkzeug. Flask is used for web-based dashboard
and public REST API.

## [PostgreSQL][]

Open-source Object-Relational DBMS with procedural programming capabilities
and very rich feature set including support for JSON.

## [Twitter Bootstrap][]

Twitter Bootstrap is a CSS/JavaScript/HTML toolkit for rapid UI prototyping
and design. It has been chosen because of it's simplicity, ease of use and
versatility.

## [jQuery][]

jQuery is a JavaScript library for HTML document traversing, event handling and
animating. It's widely used in Twitter Bootstrap so basicaly it is its inner
dependency.

## [AngularJS][]

Angular is an MVVM framework in JavaScript which has been chosen because of
it's capability to easily create event-driven client applications using HTML5.

## [varnish][]

Varnish is a HTTP reverse proxy used for load balancing, caching and on traffic
type based request routing. The main reason to use it is its lightweightness,
ease of configuration and low system requirements.

## [Zabbix][]

Zabbix is an enterprise-class open source distributed monitoring solution
for networks and applications.


[PostgreSQL]: http://www.postgresql.org/
[collectd]:   http://collectd.org/
[ZooKeeper]:  http://zookeeper.apache.org/
[varnish]:    https://www.varnish-cache.org/
[Werkzeug]:   http://werkzeug.pocoo.org/
[AngularJS]:  http://angularjs.org/
[jQuery]:     http://jquery.com/
[Twitter Bootstrap]: http://twitter.github.com/bootstrap/
[Zabbix]:     http://www.zabbix.com/
[dnsmasq]:    http://www.thekelleys.org.uk/dnsmasq/doc.html
[psycopg2]:   http://www.initd.org/psycopg/
[Sheepdog]:   http://www.osrg.net/sheepdog/
[libvirt]:    http://libvirt.org/
[KVM]:        http://www.linux-kvm.org/
[ZeroMQ]:     http://www.zeromq.org/
[Twisted]:    http://twistedmatrix.com/
[Flask]:      http://flask.pocoo.org/
[Open Virtualization Alliance]: http://openvirtualizationalliance.org/
