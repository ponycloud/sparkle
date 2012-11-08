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

        # Initialize the entities.
        self.address          = Entity(self, 'address', indexes=['network', 'vnic'])
        self.bond             = Entity(self, 'bond', indexes=['host'])
        self.cluster          = Entity(self, 'cluster', indexes=['tenant'])
        self.cluster_instance = Entity(self, 'cluster_instance', indexes=['cluster', 'instance'])
        self.cpu_profile      = Entity(self, 'cpu_profile')
        self.disk             = Entity(self, 'disk', pkey=('id', 'varchar'))
        self.extent           = Entity(self, 'extent', indexes=['volume', 'storage_pool'])
        self.host             = Entity(self, 'host')
        self.image            = Entity(self, 'image', indexes=['tenant'])
        self.instance         = Entity(self, 'instance', indexes=['cpu_profile', 'tenant'])
        self.logical_volume   = Entity(self, 'logical_volume', indexes=['storage_pool', 'raid'])
        self.member           = Entity(self, 'member', indexes=['tenant', 'user'])
        self.network          = Entity(self, 'network', indexes=['switch'])
        self.nic              = Entity(self, 'nic', pkey=('hwaddr', 'varchar'), indexes=['bond'])
        self.nic_role         = Entity(self, 'nic_role', indexes=['bond'])
        self.quota            = Entity(self, 'quota', indexes=['tenant'])
        self.raid             = Entity(self, 'raid', indexes=['host'])
        self.route            = Entity(self, 'route', indexes=['network'])
        self.storage_pool     = Entity(self, 'storage_pool')
        self.switch           = Entity(self, 'switch', nm_indexes=[('switch', 'tenant_switch', 'tenant')], protected=['tenant'])
        self.tenant           = Entity(self, 'tenant', nm_indexes=[('tenant', 'tenant_switch', 'switch')])
        self.user             = Entity(self, 'user', pkey=('email', 'varchar'))
        self.vdisk            = Entity(self, 'vdisk', indexes=['instance', 'volume'])
        self.vnic             = Entity(self, 'vnic', indexes=['instance', 'switch'])
        self.volume           = Entity(self, 'volume', indexes=['tenant', 'storage_pool'])
    # /def __init__


    def apply_changes(self, changes):
        """
        Applies set of changes.

        Changes format is `[(id, table, old_value, new_value), ...]`.

        The old_value and new_value is dict with keys 'desired' and 'current'.
        If only one part is supplied, the other one is not affected by the
        change.  This allows for desired state updates that do not affect
        current state and vice versa.
        """

        # Iterate over changes.
        for cid, table, old, new in changes:
            # Notify correct tables about the change.
            for ent in self.table_map.get(table, []):
                ent.notify(table, old, new)


    def clear(self):
        """
        Throws away all data.
        """

        for name in dir(self):
            entity = getattr(self, name)
            if isinstance(entity, Entity):
                entity.clear()
    # /def clear

# /class Model


class Entity(object):
    """
    Encapsulates collection of entities.
    """

    def __init__(self, model, name, pkey=('uuid', 'uuid'), \
                       indexes=[], nm_indexes=[], protected=[]):
        """
        Initializes the collection.

        Parameters:
            model      -- Sparkle database model instance
            name       -- name of the underlying sqlsoup entity
            pkey       -- tuple with (name, type) of the primary key
            indexes    -- list fields to index for lookup
            nm_indexes -- list of (join_left join_table join_right)
                          tuples describing N:M relation to index
            protected  -- list of fields that need to come separately from
                          the bulk uploaded by the user, primary key is
                          protected by default
        """

        # Store the arguments.
        self.model = model
        self.name = name
        self.pkey = pkey
        self.indexes = indexes
        self.nm_indexes = nm_indexes
        self.protected = protected + [pkey[0]]

        # Tables that provide N:M indexes.
        self.nm_tables = set([nm[1] for nm in self.nm_indexes])

        # Initialize through settings things to clear state.
        self.clear()

        # We need to be notified about tables changes.
        for table in [self.name] + list(self.nm_tables):
            self.model.table_map.setdefault(table, set())
            self.model.table_map[table].add(self)
    # /def __init__


    def clear(self):
        """
        Throws away all data.
        """

        # The entities.
        self.data = {}

        # Auxiliary indexes.
        self.pkeys = set()
        self.index = {}

        # Initialize the indexes.
        for idx in self.indexes:
            self.index[idx] = {}
        for local, table, remote in self.nm_indexes:
            self.index[remote] = {}
    # /def clear


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
        assert table == self.name

        for state in ('desired', 'current'):
            if old.get(state) is not None:
                # Get the primary key.
                pkey = old[state][self.pkey[0]]

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
                pkey = new[state][self.pkey[0]]

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
            if state is not None and self.pkey[0] in state:
                return state[self.pkey[0]]
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
