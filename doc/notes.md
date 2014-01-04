% Random Notes

Notes to be incorporated into documentation later on.


# Desired State On Sparkle

The only authoritative copy of desired state is kept in a PostgreSQL database.
In order to be able to serve it quickly, we also keep a copy in the Sparkle
process using native Python data structure.

Our initial calculations indicate that this will cost no more than 2GiB
for 100.000 instances, which we consider acceptable for a dedicated machine.

When the Sparkle process starts, it reads all of desired state from the
database.  Estimated time is way below 10 seconds, 30 seconds in the
absolutely worst case.

Any changes to the desired state will have to go throught the database and
be consumed by Sparkle process via PostgreSQL notification mechanism,
so that we can keep the cache coherent with the database.


# Desired State on Twilight

Attempts to apply the desired state on Twilight are retried indefinitely
until current and desired states "match".  The desired and current state
attributes of entities may differ significantly.


# Volume And Image Lifecycle

New volume can be empty or it can reference an image that will be used as
initial content.  Once the volume is initially populated, reference to image
is deleted and the volume is completely independent.

Instance cannot be started until all it's disks have been initialized.
New disks cannot be added or hot-plugged until they have also been initialized.

Images are conceptually immutable snapshots of volumes.  Single image can have
several volumes in different storage pools to increase it's availability on
different hosts.

In order to create an image, the data need to be copied either from an
existing volume or from an external storage, such as an HTTP server.


# Low-level Roadmap

## Version 1.0

### Base

 *  database with users and tenants
 *  RESTful API for Sparkle
 *  Twilight/Sparkle interaction framework
 *  Luna/Sparkle interaction framework
 *  netboot and version management via Luna
 *  fencing via Luna
 *  web-based user interface
 *  database backups with WAL replication

### Platform Management

 *  host membership
 *  network configuration
 *  local storage
 *  shared storage
 *  distributed storage

### Virtualization

 *  virtual volumes
 *  images
 *  instances on core networks
 *  virtual networks

### Installation

 *  fatter than usual Twilight image with browser, database and other
    tools required to bootstrap the environment

## Version 1.1

### "Enterprise" Features

 *  LDAP integration

### Resiliency

 *  RAID under PostgreSQL database

### Backups

 *  backup server appliance
 *  differential backups of volumes selected by user
 *  file system freeze/thaw via guest agent for safer backups
 *  creating images from backups

