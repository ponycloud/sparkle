#!/usr/bin/python -tt

"""
Sparkle Data Model
"""

def combined_keys(row, key):
    """
    Returns combined set of values under same key in both row states.
    """
    s = set()
    for state in row.values():
        if state is not None and key in state:
            s.add(state[key])
    return s


def get_pkey(row, pkey):
    """Returns (possibly composite) primary key value."""
    if isinstance(pkey, tuple):
        for k in pkey:
            if k not in row:
                return None
        return tuple([row[k] for k in pkey])
    return row.get(pkey, None)


class Model(object):
    """
    Encapsulates the whole Sparkle data model.
    """

    def __init__(self):
        """
        Initializes the model.
        """

        # Map of tables to entities that need to receive notifications.
        self.table_map = {}

        # Database entities.
        self.address          = Entity(self, 'address', indexes=['network', 'vnic'])
        self.bond             = Entity(self, 'bond', indexes=['host'])
        self.cluster          = Entity(self, 'cluster', indexes=['tenant'])
        self.cluster_instance = Entity(self, 'cluster_instance', indexes=['cluster', 'instance'])
        self.cpu_profile      = Entity(self, 'cpu_profile')
        self.disk             = Entity(self, 'disk', pkey=('id', 'varchar'), nm_indexes=[('disk', 'host_disk', 'host')])
        self.extent           = Entity(self, 'extent', indexes=['volume', 'storage_pool'])
        self.host             = Entity(self, 'host', nm_indexes=[('host', 'host_disk', 'disk'), ('host', 'host_instance', 'instance')])
        self.image            = Entity(self, 'image', indexes=['tenant'])
        self.instance         = Entity(self, 'instance', indexes=['cpu_profile', 'tenant'], nm_indexes=[('instance', 'host_instance', 'host')])
        self.logical_volume   = Entity(self, 'logical_volume', indexes=['storage_pool', 'raid'])
        self.member           = Entity(self, 'member', indexes=['tenant', 'user'])
        self.network          = Entity(self, 'network', indexes=['switch'])
        self.nic              = Entity(self, 'nic', pkey=('hwaddr', 'varchar'), indexes=['bond'])
        self.nic_role         = Entity(self, 'nic_role', indexes=['bond'])
        self.quota            = Entity(self, 'quota', indexes=['tenant'])
        self.raid             = Entity(self, 'raid', indexes=['host'])
        self.route            = Entity(self, 'route', indexes=['network'])
        self.storage_pool     = Entity(self, 'storage_pool')
        self.switch           = Entity(self, 'switch', nm_indexes=[('switch', 'tenant_switch', 'tenant')])
        self.tenant           = Entity(self, 'tenant', nm_indexes=[('tenant', 'tenant_switch', 'switch')])
        self.tenant_image     = Entity(self, 'tenant_image', pkey=(('tenant', 'image'), ('uuid', 'uuid')), indexes=['tenant', 'image'])
        self.tenant_switch    = Entity(self, 'tenant_switch', pkey=(('tenant', 'switch'), ('uuid', 'uuid')), indexes=['tenant', 'switch'])
        self.user             = Entity(self, 'user', pkey=('email', 'varchar'))
        self.vdisk            = Entity(self, 'vdisk', indexes=['instance', 'volume'])
        self.vnic             = Entity(self, 'vnic', indexes=['instance', 'switch'])
        self.volume           = Entity(self, 'volume', indexes=['tenant', 'storage_pool'])

        # Set of virtual tables that do not exist in database.
        self.virtual = set(['host_disk', 'host_instance'])

        # Virtual entities.
        self.host_disk = Entity(self, 'host_disk', pkey=(('host', 'disk'), ('uuid', 'uuid')), indexes=['host', 'disk'])
        self.host_instance = Entity(self, 'host_instance', pkey=(('host', 'instance'), ('uuid', 'uuid')), indexes=['host', 'instance'])
    # /def __init__


    def apply_changes(self, changes):
        """
        Applies set of changes.

        Changes format is `[(table, old_value, new_value), ...]`.

        The old_value and new_value is dict with keys 'desired' and 'current'.
        If only one part is supplied, the other one is not affected by the
        change.  This allows for desired state updates that do not affect
        current state and vice versa.
        """

        # Iterate over changes.
        for table, old, new in changes:
            # Notify correct tables about the change.
            for ent in self.table_map.get(table, []):
                ent.notify(table, old, new)


    def dump(self, states=['current', 'desired']):
        """Dumps specified state from all rows as a changelog."""

        out = []

        for table in dir(self):
            entity = getattr(self, table)
            if not isinstance(entity, Entity):
                continue

            for row in entity.dump(states):
                out.append(row)

        return out


    def clear(self):
        """
        Throws away all data.
        """

        for table in dir(self):
            entity = getattr(self, table)
            if isinstance(entity, Entity):
                entity.clear()
    # /def clear

# /class Model


class Entity(object):
    """
    Encapsulates collection of entities.
    """

    def __init__(self, model, table, pkey=('uuid', 'uuid'), \
                       indexes=[], nm_indexes=[]):
        """
        Initializes the collection.

        Parameters:
            model      -- Sparkle database model instance
            table      -- name of the underlying sqlsoup table
            pkey       -- tuple with (name, type) of the primary key,
                          if the key is composite, tuple of tuples
            indexes    -- list fields to index for lookup
            nm_indexes -- list of (join_left join_table join_right)
                          tuples describing N:M relation to index
        """

        # Store the arguments.
        self.model = model
        self.table = table
        self.pkey = pkey
        self.indexes = indexes
        self.nm_indexes = nm_indexes

        # Tables that provide N:M indexes.
        self.nm_tables = set([nm[1] for nm in self.nm_indexes])

        # Initialize through clearing.
        self.clear()

        # We need to be notified about table changes.
        for table in [self.table] + list(self.nm_tables):
            self.model.table_map.setdefault(table, set())
            self.model.table_map[table].add(self)
    # /def __init__


    def clear(self):
        """Throws away all data."""

        # Throw away everything.
        self.data = {}
        self.pkeys = set()
        self.index = {}

        # Initialize normal indexes.
        for idx in self.indexes:
            self.index[idx] = {}

        # Initialize N:M indexes.
        for local, table, remote in self.nm_indexes:
            self.index[remote] = {}


    def dump(self, states=['desired', 'current']):
        """Dumps specified state from all rows as a changelog."""
        return [(self.table, {}, \
                 {k: v for k, v in self.data.items() if k in states})]


    def notify(self, table, old, new):
        """
        Called when a row have been updated.

        Notification is issued for both the primary table and all
        N:M mapping tables.
        """

        # If the notification is about a join table, we have less work.
        # Plus joins like this are only possible on the desired state.
        if table in self.nm_tables:
            local, table, remote = [nm for nm in self.nm_indexes \
                                       if nm[1] == table].pop()

            if old.get('desired') is not None:
                # We need to remove the old link.
                self.index[remote][old['desired'][remote]].remove(old['desired'][local])
                if 0 == len(self.index[remote][old['desired'][remote]]):
                    del self.index[remote][old['desired'][remote]]

            if new.get('desired') is not None:
                # We need to install new link.
                self.index[remote].setdefault(new['desired'][remote], set())
                self.index[remote][new['desired'][remote]].add(new['desired'][local])

            # That's it for N:M tables, primary table handling below.
            return


        # Make sure we have been notified correctly.
        assert table == self.table

        for state in ('desired', 'current'):
            if old.get(state) is not None:
                # Get the primary key.
                pkey = get_pkey(old[state], self.pkey[0])

                # We need to remove this state from the row.
                del self.data[pkey][state]

                # It that was all that was left from this row,
                # remove the row completely.
                if 0 == len(self.data[pkey]):
                    del self.data[pkey]
                    self.pkeys.remove(pkey)

        # We might also need to remove all secondary indexes.
        pkey = self._primary_key(old)
        if pkey is not None:
            for idx in self.indexes:
                for value in combined_keys(old, idx):
                    self.index[idx][value].remove(pkey)
                    if 0 == len(self.index[idx][value]):
                        del self.index[idx][value]

        for state in ('desired', 'current'):
            if new.get(state) is not None:
                # Get the primary key.
                pkey = get_pkey(new[state], self.pkey[0])

                # We need to install the portion of the row.
                if pkey not in self.data:
                    self.data[pkey] = new
                    self.pkeys.add(pkey)
                else:
                    self.data[pkey].update(new)

        # We might also need to add associated secondary indexes.
        pkey = self._primary_key(new)
        if pkey is not None:
            for idx in self.indexes:
                for value in combined_keys(new, idx):
                    self.index[idx].setdefault(value, set())
                    self.index[idx][value].add(pkey)
    # /def notify


    def _primary_key(self, row):
        """
        Returns primary key for given row, if it contains at least one state.
        """
        for state in row.values():
            if state is not None:
                pkey = get_pkey(state, self.pkey[0])
                if pkey is not None:
                    return pkey
        return None


    def get(self, key):
        """
        Retrieves item using the primary key.
        """
        return self.data[key]


    def list(self, **keys):
        """
        Retrieves list of items using selected secondary keys.

        If the index is not known, we gracefully ignore it.
        """

        # Start with all primary keys.
        pkeys = self.pkeys

        # For every additional key, reduce the set.
        for k, v in keys.items():
            if k in self.index:
                pkeys = pkeys.intersection(self.index[k].get(v, set()))

        return [self.data[pk] for pk in pkeys]

# /class Entity

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
