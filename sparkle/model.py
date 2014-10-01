#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Model', 'OverlayModel', 'Table', 'Row']

from collections import Mapping, MutableMapping
from sparkle.schema import schema


class Model(Mapping):
    """
    PonyCloud Data Model

    This model is actually a cache with desired state from the database and
    current state sent from individual hosts.

    It is organized in tables with rows, some of the columns being indexed
    for fast lookup.  Every row have two "parts" -- the current and desired
    state portion that can be a dictionary or None, depending on presence
    of the portion in question.  Not all rows have both parts.
    """

    __slots__ = ['current', 'desired']

    def __init__(self):
        # Keep separate per-table indexed mappings for both states.
        self.desired = {}
        self.current = {}

        # Pre-define the mappings.
        for name, table in schema.tables.iteritems():
            self.desired[name] = IndexedMapping(indexes=table.index)
            self.current[name] = IndexedMapping(indexes=table.index)

    def __getitem__(self, key):
        return Table(self, key)

    def __getattr__(self, name):
        return self[name]

    def __iter__(self):
        return iter(schema.tables)

    def __len__(self):
        return len(schema.tables)

    def load(self, changes):
        """
        Apply bulk of row part changes to the model.

        Every change in the set is a 4-element-tuple of::

            ('table', 'pkey', 'desired|current', {'some': 'data'})

        Specified table must exist and only 'desired' and 'current' are
        the only allowed states.  Data must either be a mapping or None,
        with None translating to removal of that part.

        Invalid changes will produce exceptions.
        """

        for name, pkey, state, data in changes:
            if state not in ('desired', 'current'):
                raise ValueError('%r is not a valid state' % (state,))

            table = getattr(self, state)[name]

            if data is None:
                if pkey in table:
                    del table[pkey]
            else:
                table[pkey] = data

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

            # Start with filter taken from schema.
            filter = dict(endpoint.filter)

            if isinstance(pkey, basestring):
                # Ensure we have the key matching our table name if this
                # table uses a simple single-column primary key.
                filter[pkey] = keys[name]
            else:
                # Ensure all columns for composite primary key are matched.
                for subkey in pkey:
                    filter[subkey] = keys[subkey]

            # We may have a parent relation to filter by, too.
            if endpoint.parent.table:
                pname = endpoint.parent.table.name
                ppkey = endpoint.parent.table.pkey

                # Filter according to parent relation.
                if isinstance(ppkey, basestring):
                    filter[pname] = keys[pname]
                else:
                    for subkey in ppkey:
                        filter[subkey] = keys[subkey]

            # Obtain the matching row or raise an error.
            row = self[name].one(**filter)

        return row


class OverlayModel(Model):
    """
    Copy-on-write model wrapper utilizing overlay mappings.
    """

    __slots__ = ['parent', 'current', 'desired', 'callbacks']

    def __init__(self, parent):
        self.parent = parent
        self.callbacks = set()

        self.desired = {}
        self.current = {}

        for name, table in schema.tables.iteritems():
            self.desired[name] = OverlayMapping(parent.desired[name])
            self.current[name] = OverlayMapping(parent.current[name])

    def add_callback(self, cb):
        """
        Add callback function that will be called with overview of all
        transaction changes in the form of old and new row tuples right
        before the changes staged in the overlay are committed.

        The function can yield to wait for the transaction to proceed and
        then continue after the transaction have completed.
        """

        self.callbacks.add(cb)

    def remove_callback(self, cb):
        """Discard a previously added callback function."""
        self.callbacks.discard(cb)

    def commit(self):
        """
        Apply all changes in the overlay to the underlying model.
        """

        modified = set()

        for name in schema.tables:
            for key in self.desired[name].deleted:
                modified.add((name, key))

            for key in self.current[name].deleted:
                modified.add((name, key))

            for key in self.desired[name].overlay:
                modified.add((name, key))

            for key in self.current[name].overlay:
                modified.add((name, key))

        changes = []

        for name, key in modified:
            if key in self.parent[name]:
                old = self.parent[name][key]
            else:
                old = Row(self.parent[name], key)

            if key in self[name]:
                new = self[name][key]
            else:
                new = Row(self[name], key)

            changes.append((old, new))

        callbacks = [c(changes) for c in self.callbacks]

        for callback in callbacks:
            if hasattr(callback, 'next'):
                try:
                    next(callback)
                except StopIteration:
                    pass

        for name in schema.tables:
            self.desired[name].commit()
            self.current[name].commit()

        for callback in callbacks:
            if hasattr(callback, 'next'):
                try:
                    next(callback)
                except StopIteration:
                    pass

    def rollback(self):
        """
        Discard the overlay data and reset to the underlying model state.
        """

        for name in schema.tables:
            self.desired[name].rollback()
            self.current[name].rollback()


class Part(object):
    """
    Read-only getattr wrapper for current/desired state data.
    """

    __slots__ = ['data']

    def __init__(self, data):
        self.data = data

    def __getattr__(self, name):
        if self.data is None:
            return None

        return self.data.get(name)


class Table(Mapping):
    """
    Abstraction of a table that takes data from the model.

    Provides some querying capabilities combining data from both
    current and desired state, returning Row objects.
    """

    __slots__ = ['model', 'name']

    def __init__(self, model, name):
        self.model = model
        self.name = name

    @property
    def schema(self):
        return schema.tables[self.name]

    @property
    def current(self):
        return self.model.current[self.name]

    @property
    def desired(self):
        return self.model.desired[self.name]

    @property
    def m(self):
        return self.model

    def __getitem__(self, key):
        if key not in (self.desired or {}) and \
           key not in (self.current or {}):
            raise KeyError(key)

        return Row(self, key)

    def __iter__(self):
        for key in self.desired:
            yield key

        for key in self.current:
            if key not in self.desired:
                yield key

    def __len__(self):
        return len(set().union(self.current, self.desired))

    def list(self, **fields):
        """
        Query rows with matching fields in either desired or current state.
        """

        return [Row(self, k) for k in self.list_keys(**fields)]

    def list_keys(self, **fields):
        """
        Same as list, but return only the primary keys.
        """

        result = None

        if not fields:
            return set(self)

        for f, fv in fields.iteritems():
            subresult = set(self.desired.lookup(f, fv))
            subresult.update(self.current.lookup(f, fv))

            if result is None:
                result = subresult
            else:
                result.intersection_update(subresult)

            if not result:
                return result

        return result

    def one(self, **keys):
        """
        Same as list(), but return just one item.
        If there are more rows or no rows at all, raises KeyError.
        """

        items = self.list(**keys)

        if len(items) == 0:
            raise KeyError('no matching rows found')

        if len(items) > 1:
            raise KeyError('too many matching rows found')

        return items[0]


class Row(object):
    def __init__(self, table, pkey):
        self.table = table
        self.pkey = pkey

    @property
    def model(self):
        return self.table.model

    m = model

    @property
    def desired(self):
        return self.table.model.desired[self.table.name].get(self.pkey)

    @property
    def d(self):
        return Part(self.desired)

    @property
    def current(self):
        return self.table.model.current[self.table.name].get(self.pkey)

    @property
    def c(self):
        return Part(self.current)

    def check_filter(self, filter):
        for key, value in filter.iteritems():
            if self.get_desired(key, value) != value or \
               self.get_current(key, value) != value:
                return False
        return True

    def get_access(self):
        access = set()

        # Discard all endpoints that are tenant-specific.
        for endpoint in self.table.schema.endpoints.itervalues():
            if endpoint.access.startswith('tenant/') or \
               endpoint.access.startswith('user/'):
                continue

            # Discard rows that doesn't match filter
            if not self.check_filter(endpoint.filter):
                continue

            access.add(endpoint.access)

        return access

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

            # Discard rows that doesn't match filter.
            if not self.check_filter(endpoint.filter):
                continue

            # Traverse rows up to the tenant.
            row = self
            mnt = endpoint

            while mnt.table is not None:
                if mnt.table.name == 'tenant':
                    tenants.add(row.pkey)
                    break

                key = row.get_current(mnt.parent.table.name)
                key = row.get_desired(mnt.parent.table.name, key)

                if key is None:
                    break

                row = row.table.model[mnt.parent.table.name].get(key)
                mnt = mnt.parent

                if row is None:
                    break

        return tenants

    def get_current(self, key, default=None):
        """
        Get value for given key in the current state.
        """

        if self.current is None:
            return default

        return self.current.get(key, default)

    def get_desired(self, key, default=None):
        """
        Get value for given key in the current state.
        """

        if self.desired is None:
            return default

        return self.desired.get(key, default)

    def get(self, key, default=None):
        """
        Get value for given key in either desired or current state.
        """

        desired = self.get_desired(key)

        if desired is None:
            return self.get_current(key)

        return desired

    def to_dict(self):
        result = {}

        if self.desired:
            result['desired'] = dict(self.desired)

        if self.current:
            result['current'] = dict(self.current)

        return result


class OverlayMapping(MutableMapping):
    """
    Copy-on-write view that uses two indexed mappings.

    Normally works in the read-from-both, write-to-one mode,
    but can also flush the differences to the underlying mapping.
    """

    __slots__ = ['parent', 'overlay', 'deleted']

    def __init__(self, parent):
        self.parent = parent
        self.overlay = IndexedMapping(parent.idx)
        self.deleted = set()

    def __getitem__(self, key):
        try:
            return self.overlay[key]
        except KeyError:
            if key in self.deleted:
                raise KeyError(key)
            return self.parent[key]

    def __setitem__(self, key, value):
        self.overlay[key] = value
        if key in self.parent:
            self.deleted.add(key)

    def __delitem__(self, key):
        if key in self.overlay:
            del self.overlay[key]
        elif key in self.deleted:
            raise KeyError(key)
        elif key in self.parent:
            self.deleted.add(key)
        else:
            raise KeyError(key)

    def __contains__(self, key):
        if key in self.overlay:
            return True

        return key not in self.deleted and key in self.parent

    def __iter__(self):
        for key in self.overlay:
            yield key

        for key in self.parent:
            if key not in self.deleted and key not in self.overlay:
                yield key

    def __len__(self):
        return len(self.overlay) + len(self.parent) - len(self.deleted)

    def __repr__(self):
        return repr(dict(self))

    def lookup(self, name, value):
        parent = self.parent.lookup(name, value)
        overlay = self.overlay.lookup(name, value)
        return parent.difference(self.deleted).union(overlay)

    def kwlookup(self, **fields):
        parent = self.parent.kwlookup(**fields)
        overlay = self.overlay.kwlookup(**fields)
        return parent.difference(self.deleted).union(overlay)

    def filter(self, **fields):
        return {k: self[k] for k in self.kwlookup(**fields)}

    def rollback(self):
        """
        Discard the overlay data and reset to the underlying mapping state.
        """

        self.deleted = set()
        self.overlay = IndexedMapping(self.parent.idx)

    def commit(self):
        """
        Apply all changes in the overlay to the underlying mapping.
        """

        for key in self.deleted:
            if key not in self.overlay:
                del self.parent[key]

        self.parent.update(self.overlay)
        self.rollback()


class IndexedMapping(MutableMapping):
    """
    Dictionary-style container that indexes some of the values' fields.

    Values must conform to the Mapping interface so that they can be
    indexed.  Not all indexed fields need to be present in every value.
    """

    __slots__ = ['data', 'idx']

    def __init__(self, indexes=[]):
        self.data = {}
        self.idx = {i: {} for i in indexes}

    def index(self, key, value):
        for i, index in self.idx.iteritems():
            if i in value:
                index.setdefault(value[i], set()).add(key)

    def unindex(self, key):
        value = self.data[key]
        for i, index in self.idx.iteritems():
            if i in value:
                iv = value[i]
                index[iv].discard(key)
                if not index[iv]:
                    del index[iv]

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        if not isinstance(value, Mapping):
            raise ValueError('IndexedMapping can only contain other mappings')

        if key in self.data:
            del self[key]

        self.data[key] = value
        self.index(key, value)

    def __delitem__(self, key):
        self.unindex(key)
        del self.data[key]

    def __contains__(self, key):
        return key in self.data

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def __repr__(self):
        return repr(self.data)

    def lookup(self, name, value):
        if name not in self.idx:
            raise ValueError('missing index on %r' % (name,))

        return self.idx[name].get(value, set())

    def kwlookup(self, **fields):
        result = None

        if not fields:
            return set(self)

        for f, fv in fields.iteritems():
            if result is None:
                result = set(self.lookup(f, fv))
            else:
                result.intersection_update(self.lookup(f, fv))

            if not result:
                return result

        return result

    def filter(self, **fields):
        return {k: self.data[k] for k in self.kwlookup(**fields)}


# vim:set sw=4 ts=4 et:
