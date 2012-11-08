% Random Notes

Notes to be incorporated into documentation later on.


# Desired State On Sparkle

The only authoritative copy of desired state is kept in a PostgreSQL database.
In order to be able to serve it quickly, we also keep a copy of pre-rendered
JSON structures in the Sparkle process.

Our initial calculations indicate that this will cost no more than 2GiB
for 100.000 instances, which we consider acceptable as by this point a
dedicated controller will be deployed.

When the Sparkle process starts, it reads all of desired state from the
database, rendering the JSON structures.  Estimated time is way below 10
seconds, 30 seconds in the absolutely worst case.

Any changes to the desired state will have to go throught the Sparkle process,
so that we can keep the cache coherent with the database.  Since we have
previously decided to use database triggers to maintain consistency, we need
a mechanism that will inform us about indirect changes to top-level entities.

**TODO:** Define top-level entities.

This mechanism will be an in-memory table `changes`, that will be filled with
a changelog of sorts, generated by triggers defined on most of the other
tables.  After all intended mutations are done, this table is consulted for
the complete list of all changes this transaction will cause.  This table will
then be emptied and all changes entities will be queried and rendered.

With changes in an uncommited transaction in the database and changed
top-level entities in the Sparkle, the mutator thread will then acquire the
rwlock protecting global dictionaries with aforementioned cache.  Once having
an exclussive access, the mutator thread will commit the database transaction.
In case of failure, the rwlock is released and exception propagated to the
user. In case of success, the cache is updated with pre-rendered entities
and all deleted entites are removed.  The modified entities are queued to
interested Twilights and the rwlock is released for other mutators and
readers to continue.

**TODO:** Define mutators and readers.


# Desired State on Twilight

Attempts to apply the desired state on Twilight are retried indefinitely
until current and desired states match.


# Volume And Image Lifecycle

New volume can be empty or it can reference an image that will be used as
initial content.  Once the volume is initially populated, reference to image
is deleted and the volume is completely independent.

TODO: Instance cannot be started until all it's disks have been initialized.
      New disks cannot be added or hot-plugged until they have been
      initialized as well.

Images are conceptually immutable snapshots of volumes.  Every image has one
primary volume and one optional volume in every other Sheepdog storage pool.

In order to create an image, the data need to be copied either from an
existing volume or from an external storage, such as an HTTP server.

Volumes won't be populated until the image is fully created.


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
