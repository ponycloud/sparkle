#!/usr/bin/python -tt

"""
Sparkle Data Model
"""

class Model(object):
    """
    Encapsulates the whole Sparkle data model.
    """

    def __init__(self, db):
        """
        Initializes the model.
        """

        # Save attributes.
        self.db = db

        # Map of tables to entities that need to receive notifications.
        self.table_map = {}

        # Initialize the entities.
        self.address         = Entity(self, 'address', indexes=['network', 'vnic'])
        self.cluster         = Entity(self, 'cluster', indexes=['tenant'], nm_indexes=[('cluster', 'cluster_instance', 'instance')])
        self.cpu_profile     = Entity(self, 'cpu_profile')
        self.disk            = Entity(self, 'disk', pkey=('id', 'varchar'))
        self.extent          = Entity(self, 'extent', indexes=['volume', 'storage_pool'])
        self.host            = Entity(self, 'host')
        self.image           = Entity(self, 'image', indexes=['tenant'])
        self.instance        = Entity(self, 'instance', indexes=['cpu_profile', 'tenant'], nm_indexes=[('instance', 'cluster_instance', 'cluster')])
        self.nic_role        = Entity(self, 'nic_role', indexes=['bond'])
        self.logical_volume  = Entity(self, 'logical_volume', indexes=['storage_pool', 'raid'])
        self.member          = Entity(self, 'member', indexes=['tenant', 'user'])
        self.network         = Entity(self, 'network', indexes=['switch'])
        self.nic             = Entity(self, 'nic', pkey=('hwaddr', 'varchar'), indexes=['bond'])
        self.bond            = Entity(self, 'bond', indexes=['host'])
        self.quota           = Entity(self, 'quota', indexes=['tenant'])
        self.raid            = Entity(self, 'raid', indexes=['host'])
        self.route           = Entity(self, 'route', indexes=['network'])
        self.storage_pool    = Entity(self, 'storage_pool')
        self.switch          = Entity(self, 'switch', nm_indexes=[('switch', 'tenant_switch', 'tenant')], protected=['tenant'])
        self.tenant          = Entity(self, 'tenant', nm_indexes=[('tenant', 'tenant_switch', 'switch')])
        self.user            = Entity(self, 'user', pkey=('email', 'varchar'))
        self.vdisk           = Entity(self, 'vdisk', indexes=['instance', 'volume'])
        self.vnic            = Entity(self, 'vnic', indexes=['instance', 'switch'])
        self.volume          = Entity(self, 'volume', indexes=['tenant', 'storage_pool'])
    # /def __init__


    def load(self):
        """
        Perform initial load from the database on all entities.
        """

        self.clear()

        for table, entities in self.table_map.items():
            for row in getattr(self.db, table).all():
                row = {c.name: getattr(row, c.name) for c in row.c}
                for ent in entities:
                    ent.notify(table, None, row)
    # /def load


    def apply_changes(self, changes):
        """
        Applies set of changes.

        Changes format is `[(id, table, old_value, new_value), ...]`.
        """

        # Iterate over changes.
        for cid, table, old, new in changes:
            # Notify correct tables about the change.
            for ent in self.table_map.get(table, []):
                ent.notify(table, old, new)
    # /def apply_changes

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
        if table in self.nm_tables:
            local, table, remote = [nm for nm in self.nm_indexes \
                                       if nm[1] == table].pop()

            if old is not None:
                # We need to remove the old link.
                self.index[remote][old[remote]].remove(old[local])
                if 0 == len(self.index[remote][old[remote]]):
                    del self.index[remote][old[remote]]

            if new is not None:
                # We need to install new link.
                self.index[remote].setdefault(new[remote], set())
                self.index[remote][new[remote]].add(new[local])

            # That's it for N:M tables, primary table handling below.
            return


        # Make sure we have been notified correctly.
        assert table == self.name

        if old is not None:
            # Get the primary key.
            pkey = old[self.pkey[0]]

            # We need to remove the old row first.
            del self.data[pkey]
            self.pkeys.remove(pkey)

            # We also need to remove all associated secondary indexes.
            for idx in self.indexes:
                value = old[idx]
                self.index[idx][value].remove(pkey)
                if 0 == len(self.index[idx][value]):
                    del self.index[idx][value]

        if new is not None:
            # Get the primary key.
            pkey = new[self.pkey[0]]

            # We need to install the new row now.
            self.data[pkey] = new
            self.pkeys.add(pkey)

            # And we also need to update all secondary indexes.
            for idx in self.indexes:
                value = new[idx]
                self.index[idx].setdefault(value, set())
                self.index[idx][value].add(pkey)
    # /def notify

    def get(self, key):
        """
        Retrieves item using the primary key.
        """
        return self.data[key]
    # /def get


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
    # /def list

# /class Entity

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
