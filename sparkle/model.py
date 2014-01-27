#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Model']


from sparkle.schema import schema


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
        for name, table in schema.tables.iteritems():
            self[name] = Table(self, name, table)


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


    def path_row(self, path, keys):
        """
        Return row on given path (from schema endpoint tree).
        """

        row = None
        endpoint = schema.root

        for elem in path:
            endpoint = endpoint.children[elem]
            name = endpoint.table.name
            pkey = endpoint.table.pkey

            if endpoint.parent.table is None:
                filter = dict(endpoint.filter)
                filter.update({pkey: keys[name]})
            else:
                pname = endpoint.parent.table.name
                filter = dict(endpoint.filter)
                filter.update({
                    pkey: keys[name],
                    pname: keys[pname],
                })

            row = self[name].one(**filter)

        return row
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

    def __init__(self, model, name, schema):
        """Prepare internal data structures of the table."""

        # Store necessary attributes.
        self.model  = model
        self.name   = name
        self.schema = schema

        # Start with empty indexes.
        self.index = {i: {'desired': {}, 'current': {}} \
                      for i in self.schema.index}

        # Callbacks that subscribe to row events.
        self.before_row_update_callbacks = []
        self.after_row_update_callbacks = []
        self.create_state_callbacks = []
        self.update_state_callbacks = []
        self.delete_state_callbacks = []
        self.before_delete_state_callbacks = []

    def primary_key(self, row):
        """Returns primary key for specified row dictionary."""
        if isinstance(self.schema.pkey, basestring):
            return row[self.schema.pkey]
        return tuple([row[k] for k in self.schema.pkey])


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
        """register function to call after a state is updated."""
        for rec in self.update_state_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.update_state_callbacks.append({'callback': callback,
                                            'states': states})


    def on_delete_state(self, callback, states=['desired', 'current']):
        """register function to call after a state is deleted."""
        for rec in self.delete_state_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.delete_state_callbacks.append({'callback': callback,
                                            'states': states})

    def on_before_delete_state(self, callback, states=['desired', 'current']):
        """register function to call after a state is deleted."""
        for rec in self.before_delete_state_callbacks:
            if rec['callback'] == callback:
                rec['states'] = states
                return

        self.before_delete_state_callbacks.append({'callback': callback,
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
            self[pkey] = row = Row(self, pkey)

        # Fire callbacks to inform subscribers that the row will change.
        for rec in self.before_row_update_callbacks:
            if state in rec['states']:
                rec['callback'](self, row)

        # {create, update, delete} callbacks interested in this event.
        state_callbacks = []

        if part is None:
            if getattr(row, state) is not None:
                # Fire callbacks just before delete
                for rec in self.before_delete_state_callbacks:
                    if state in rec['states']:
                        rec['callback'](self, row)

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


    def list(self, **keys):
        """
        Return rows with indexed columns matching given keys.
        Asking for non-indexed keys will result in a failure.
        """

        selection = None
        for k, v in keys.iteritems():
            subselection = set()
            for state in ('desired', 'current'):
                if v in self.index[k][state]:
                    subselection.update(self.index[k][state][v])

            if selection is None:
                selection = subselection
            else:
                selection.intersection_update(subselection)

        if selection is None:
            return self.values()

        return [self[k] for k in selection]


    def one(self, **keys):
        """
        Same as list(), but returns just one item.

        If the item is not found, or there are multiple such items,
        raises KeyError.
        """

        items = self.list(**keys)

        if len(items) == 0:
            raise KeyError('no matching rows found')

        if len(items) > 1:
            raise KeyError('too many matching rows found')

        return items[0]

    def get_watch_handler(self, model, assign_callback):
        def f(table, row):
            assign_callback(table, row, row.get_desired('host'))
        return f

# /class Table


class Row(object):
    # Each row have two parts, one for each "state".
    __slots__ = ['table', 'pkey', 'desired', 'current']


    def __init__(self, table, pkey):
        """Initializes the row."""
        self.pkey = pkey
        self.table = table
        self.desired = None
        self.current = None


    def get_tenants(self):
        """
        Return set of tenants that can access this row.

        If the row cannot be accessed by any tenant, return an empty set.
        Such rows are for example public images or alicorn-limited hosts.
        """

        tenants = set()

        for endpoint in self.table.schema.endpoints.itervalues():
            # Discard all endpoints that are not tenant-specific.
            if not endpoint.access.startswith('tenant/'):
                continue

            # Traverse rows up to the tenant.
            row = self
            mnt = endpoint

            while mnt.parent.table is not None:
                if mnt.table.name == 'tenant':
                    tenants.add(row.pkey)
                    break

                key = self.get_current(mnt.parent.table.name)
                key = self.get_desired(mnt.parent.table.name, key)

                if key is None:
                    break

                row = row.table.model[mnt.parent.table.name][key]
                mnt = mnt.parent

        return tenants


    def get_current(self, key, default=None):
        """Get value for given key in the current state."""
        if self.current is None or key not in self.current:
            return default
        return self.current[key]


    def get_desired(self, key, default=None):
        """Get value for given key in the current state."""
        if self.desired is None or key not in self.desired:
            return default
        return self.desired[key]


    def index(self, table):
        """Index the row into the table's indexes."""
        for state in ('desired', 'current'):
            for idx in table.schema.index:
                part = getattr(self, state)
                if part is not None and idx in part:
                    table.index[idx][state].setdefault(part[idx], set())
                    table.index[idx][state][part[idx]].add(self.pkey)


    def unindex(self, table):
        """Remove the row from table's indexes."""
        for state in ('desired', 'current'):
            for idx in table.schema.index:
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


# vim:set sw=4 ts=4 et:
