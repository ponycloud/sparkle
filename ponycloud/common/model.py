#!/usr/bin/python -tt

__all__ = ['Model']

class Model(dict):
    """
    PonyCloud Data Model

    This model holds both the current and desired state of all managed
    entities plus some join tables.  Most of the desired state corresponds
    to database tables, rest are virtual tables that only exist in memory.
    Current state resides in memory only and copies desired state entity
    primary keys when not completely standalone.
    """

    def __init__(self):
        """Constructs the model."""

        # Prepare all model tables.
        self.update({t.name: t() for t in TABLES})

        # Let tables watch other tables for some relations to work.
        for table in self.values():
            table.add_watches(self)


    def dump(self, states=['desired', 'current']):
        """
        Dump given states from all table rows.

        The output format (compatible with Model.load) is
        `[(table, state, pkey, part), ...]`.
        """
        out = []

        for name, table in self.iteritems():
            for row in table.itervalues():
                for state in states:
                    if getattr(row, state) is not None:
                        out.append((name, row.pkey, state, getattr(row, state)))

        return out


    def load(self, data):
        """Load previously dumped data."""
        for name, pkey, state, part in data:
            self[name].update_row(pkey, state, part)
# /class Model


class Table(dict):
    """
    Data Model Table

    Whole model is organized into indexed tables with changes
    propagated by a notification system.

    Every table have a unique primary key or set of them,
    as in case of join tables.  Any of the columns can also
    be indexed for queries.
    """

    # Name of the table within the model.
    name = None

    # True if not database backed.
    virtual = False

    # Name of the primary key column or a tuple if composite.
    pkey = 'uuid'

    # Columns to index rows by.
    indexes = []

    # Join tables for additional indexing.
    # Every item is in the form `{'table': ('local', 'remote')}`,
    # where `local` is the column refering to the local primary key and
    # `remote` the column to index by.
    nm_indexes = {}


    def __init__(self):
        """Prepare internal data structures of the table."""

        # Start with empty indexes.
        self.index = {i: {'desired': {}, 'current': {}} \
                      for i in self.indexes}
        self.nm_index = {r: {'desired': {}, 'current': {}} \
                         for l, r in self.nm_indexes.values()}

        # Callbacks that subscribe to row events.
        self.before_row_update_callbacks = []
        self.after_row_update_callbacks = []
        self.create_state_callbacks = []
        self.update_state_callbacks = []
        self.delete_state_callbacks = []


    @classmethod
    def primary_key(cls, row):
        """Returns primary key for specified row dictionary."""
        if isinstance(cls.pkey, tuple):
            return tuple([row[k] for k in cls.pkey])
        return row[cls.pkey]


    def add_watches(self, model):
        """Called to give table chance to watch other tables."""

        # Receive notifications about changes in join tables
        # and use them to build nm indexes.
        for table in self.nm_indexes:
            model[table].on_before_row_update(self.nm_unindex_row)
            model[table].on_after_row_update(self.nm_index_row)


    def on_before_row_update(self, callback, states=['desired', 'current']):
        """Register function to call before modifying a row."""
        for rec in self.before_row_update_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.before_row_update_callbacks.append({'callback': callback,
                                                 'states': states})


    def on_after_row_update(self, callback, states=['desired', 'current']):
        """Register function to call after a row is modified."""
        for rec in self.after_row_update_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.after_row_update_callbacks.append({'callback': callback,
                                                 'states': states})


    def on_create_state(self, callback, states=['desired', 'current']):
        """Register function to call after a state is created."""
        for rec in self.create_state_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.create_state_callbacks.append({'callback': callback,
                                            'states': states})

    def on_update_state(self, callback, states=['desired', 'current']):
        """Register function to call after a state is updated."""
        for rec in self.update_state_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.update_state_callbacks.append({'callback': callback,
                                            'states': states})


    def on_delete_state(self, callback, states=['desired', 'current']):
        """Register function to call after a state is deleted."""
        for rec in self.delete_state_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.delete_state_callbacks.append({'callback': callback,
                                            'states': states})


    def update_row(self, pkey, state, part):
        """
        Update/patch table row.

        Partial row contents are used to patch the row in question.
        If the part value is None, the specified state is completely
        removed and if the row have no states, it is deleted completely.
        """

        if pkey in self:
            # Row already exists, unindex it so that it can be modified.
            row = self[pkey]
            row.unindex(self)
        else:
            # Create new row object and add it to the table.
            self[pkey] = row = Row(pkey)

        # Fire callbacks to inform subscribers that the row will change.
        for rec in self.before_row_update_callbacks:
            if state in rec['states']:
                rec['callback'](self, row)

        # {create, update, delete} callbacks interested in this event.
        state_callbacks = []

        if part is None:
            if getattr(row, state) is not None:
                state_callbacks = self.delete_state_callbacks
            setattr(row, state, None)
        else:
            # Patch the corresponding row part.
            if getattr(row, state) is None:
                state_callbacks = self.create_state_callbacks
                setattr(row, state, part)
            else:
                state_callbacks = self.update_state_callbacks
                getattr(row, state).update(part)

        if row.desired is None and row.current is None:
            # Delete the row completely.
            del self[pkey]
        else:
            # Index the updated row.
            row.index(self)

        # Fire both after-row and state callbacks.
        for rec in self.after_row_update_callbacks + state_callbacks:
            if state in rec['states']:
                rec['callback'](self, row)


    def nm_unindex_row(self, table, row):
        """Unindexes join table row."""

        local, remote = self.nm_indexes[table.name]

        for state in ('desired', 'current'):
            part = getattr(row, state)
            if part is not None and remote in part and local in part:
                if part[remote] in table.nm_index[remote][state]:
                    self.nm_index[remote][state][part[remote]].remove(part[local])
                    if 0 == len(self.nm_index[remote][state][part[remote]]):
                        del self.nm_index[remote][state][part[remote]]


    def nm_index_row(self, table, row):
        """Indexes join table row."""

        local, remote = self.nm_indexes[table.name]

        for state in ('desired', 'current'):
            part = getattr(row, state)
            if part is not None and remote in part and local in part:
                self.nm_index[remote][state].setdefault(part[remote], set())
                self.nm_index[remote][state][part[remote]].add(part[local])


    def list(self, **keys):
        """Return rows with indexed columns matching given keys."""

        # None here means all rows, so that we don't have to maintain
        # redundant index of all primary keys.
        selection = None

        for k, v in keys.items():
            if k not in self.index and k not in self.nm_index:
                continue

            subselection = set()
            for idx in self.index, self.nm_index:
                for state in ('desired', 'current'):
                    if k in idx and v in idx[k][state]:
                        subselection.update(idx[k][state][v])

            if selection is None:
                selection = subselection
            else:
                selection.intersection_update(subselection)

        if selection is None:
            return self.values()
        return [self[k] for k in selection]


    def one(self, default=None, **keys):
        """
        Same as list(), but returns just one item.

        If the item is not found, or there are multiple such items,
        returns None or other configured default value.
        """
        items = self.list(**keys)
        if len(items) != 1:
            return default
        return items[0]
# /class Table


class Row(object):
    # Each row have two parts, one for each "state".
    __slots__ = ['pkey', 'desired', 'current']


    def __init__(self, pkey):
        """Initializes the row."""
        self.pkey = pkey
        self.desired = None
        self.current = None


    def __getitem__(self, key):
        """Returns key from either state, desired first."""

        if self.desired is not None and key in self.desired:
            return self.desired[key]

        if self.current is not None and key in self.current:
            return self.current[key]

        raise KeyError('key %s not found in either state' % key)


    def get(self, key, default=None):
        """Same as __getitem__(), but returns default instead of failing."""
        try:
            return self[key]
        except KeyError:
            return default


    def index(self, table):
        """Index the row into the table's indexes."""
        for state in ('desired', 'current'):
            for idx in table.indexes:
                part = getattr(self, state)
                if part is not None and idx in part:
                    table.index[idx][state].setdefault(part[idx], set())
                    table.index[idx][state][part[idx]].add(self.pkey)


    def unindex(self, table):
        """Remove the row from table's indexes."""
        for state in ('desired', 'current'):
            for idx in table.indexes:
                part = getattr(self, state)
                if part is not None and idx in part:
                    if part[idx] in table.index[idx][state]:
                        table.index[idx][state][part[idx]].remove(self.pkey)
                        if 0 == len(table.index[idx][state][part[idx]]):
                            del table.index[idx][state][part[idx]]


    def to_dict(self):
        return {'desired': self.desired, 'current': self.current}


    def __repr__(self):
        desired = ' +desired' if self.desired is not None else ''
        current = ' +current' if self.current is not None else ''
        return '<Row %s%s%s>' % (self.pkey, desired, current)
# /class Row


class Address(Table):
    name = 'address'
    indexes = ['network', 'vnic']


class Bond(Table):
    name = 'bond'
    indexes = ['host', 'bond_name']


class Cluster(Table):
    name = 'cluster'
    indexes = ['tenant']


class ClusterInstance(Table):
    name = 'cluster_instance'
    indexes = ['cluster', 'instance']


class CPUProfile(Table):
    name = 'cpu_profile'
    nm_indexes = {'host_cpu_profile': ('cpu_profile', 'host')}


class Disk(Table):
    name = 'disk'
    pkey = 'id'
    indexes = ['raid']
    nm_indexes = {'host_disk': ('disk', 'host')}


class Extent(Table):
    name = 'extent'
    indexes = ['volume', 'storage_pool']


class Host(Table):
    name = 'host'
    nm_indexes = {'host_disk':     ('host', 'disk'),
                  'host_instance': ('host', 'instance')}


class Image(Table):
    name = 'image'
    indexes = ['tenant']
    nm_indexes = {'tenant_image': ('image', 'tenant')}


class Instance(Table):
    name = 'instance'
    indexes = ['cpu_profile', 'tenant']
    nm_indexes = {'host_instance': ('instance', 'host')}


class LogicalVolume(Table):
    name = 'logical_volume'
    indexes = ['storage_pool', 'raid']


class Member(Table):
    name = 'member'
    indexes = ['tenant', 'user']


class Network(Table):
    name = 'network'
    indexes = ['switch']


class NIC(Table):
    name = 'nic'
    pkey = 'hwaddr'
    indexes = ['bond', 'nic_name']


class NICRole(Table):
    name = 'nic_role'
    indexes = ['bond', 'vlan_name', 'bridge_name']


class Quota(Table):
    name = 'quota'
    indexes = ['tenant']


class RAID(Table):
    name = 'raid'
    indexes = ['host']


class Route(Table):
    name = 'route'
    indexes = ['network']


class StoragePool(Table):
    name = 'storage_pool'


class Switch(Table):
    name = 'switch'
    indexes = ['tenant']
    nm_indexes = {'tenant_switch': ('switch', 'tenant')}


class Tenant(Table):
    name = 'tenant'
    nm_indexes = {'tenant_switch': ('tenant', 'switch')}


class TenantImage(Table):
    name = 'tenant_image'
    pkey = ('tenant', 'image')
    indexes = ['tenant', 'image']


class TenantSwitch(Table):
    name = 'tenant_switch'
    pkey = ('tenant', 'switch')
    indexes = ['tenant', 'switch']


class User(Table):
    name = 'user'
    pkey = 'email'


class VDisk(Table):
    name = 'vdisk'
    indexes = ['instance', 'volume']


class VNIC(Table):
    name = 'vnic'
    indexes = ['instance', 'switch']


class Volume(Table):
    name = 'volume'
    indexes = ['tenant', 'storage_pool']
    nm_indexes = {'host_volume': ('volume', 'host')}


class HostDisk(Table):
    virtual = True
    name = 'host_disk'
    pkey = ('host', 'disk')
    indexes = ['host', 'disk']


class HostInstance(Table):
    virtual = True
    name = 'host_instance'
    pkey = ('host', 'instance')
    indexes = ['host', 'instance']


class HostCPUProfile(Table):
    virtual = True
    name = 'host_cpu_profile'
    indexes = ['host', 'cpu_profile']


class HostVolume(Table):
    virtual = True
    name = 'host_volume'
    indexes = ['host', 'volume', 'type']


TABLES = [Address, Bond, Cluster, ClusterInstance, CPUProfile, Disk,
          Extent, Host, Image, Instance, LogicalVolume, Member, Network,
          NIC, NICRole, Quota, RAID, Route, StoragePool, Switch, Tenant,
          TenantImage, TenantSwitch, User, VDisk, VNIC, Volume, HostDisk,
          HostInstance, HostCPUProfile, HostVolume]


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
