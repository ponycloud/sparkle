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

        # Original states of the row parts before the transaction.
        self.undo = {}

        # New states of the row parts after the transaction.
        self.redo = {}

        # Callbacks that wish to be notified about changed rows when
        # the transaction is committed.
        self.callbacks = set()

    def add_callback(self, callback):
        """
        Add callback to be notified about row changes.

        Every transaction will execute the callback for every changed row
        with old state of the row as the first and new state of the row as
        the second argument.
        """

        self.callbacks.add(callback)

    def remove_callback(self, callback):
        """Remove previously added callback."""
        self.callbacks.discard(callback)

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
        """
        Load previously dumped data.

        Do not forget to `commit()` the changes to the model afterwards.
        """

        for name, pkey, state, part in data:
            self[name].replace_row(self[name].changed_row(pkey, state, part))

    def commit(self):
        """
        Run all pending callbacks and drop the undo data.
        """

        for key, new_row in self.redo.iteritems():
            old_row = self.undo[key]
            for callback in self.callbacks:
                callback(old_row, new_row)

        self.undo = {}
        self.redo = {}

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

    def changed_row(self, pkey, state, part):
        """
        Create modified row object from a change and table data.
        """

        if pkey in self:
            row = self[pkey].clone()
        else:
            row = Row(self, pkey)

        setattr(row, state, part)
        return row

    def replace_row(self, new_row):
        """
        Replace row with a modified one.
        """

        pkey = new_row.pkey

        if pkey in self:
            old_row = self[pkey]
            old_row.unindex(self)
        else:
            old_row = Row(self, pkey)

        if new_row.desired is None and new_row.current is None:
            if pkey in self:
                del self[pkey]
        else:
            self[pkey] = new_row
            new_row.index(self)

        if not (self.name, pkey) in self.model.undo:
            self.model.undo[(self.name, pkey)] = old_row

        self.model.redo[(self.name, pkey)] = new_row

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


class Row(object):
    # Each row have two parts, one for each "state".
    __slots__ = ['table', 'pkey', 'desired', 'current']


    def __init__(self, table, pkey, desired=None, current=None):
        """
        Initializes the row.
        """

        self.pkey = pkey
        self.table = table
        self.desired = desired
        self.current = current

    def clone(self):
        """Clone an existing row."""
        return Row(self.table, self.pkey, self.desired, self.current)

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

            while mnt.table is not None:
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


# vim:set sw=4 ts=4 et:
