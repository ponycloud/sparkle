#!/usr/bin/python -tt

from __future__ import unicode_literals

import re
import jsonschema

from sqlalchemy.exc import DataError
from sqlalchemy.orm.exc import UnmappedInstanceError
from pprint import pformat
from uuid import UUID, uuid4
from collections import Mapping, MutableMapping, Sequence, MutableSequence

from ponycloud.common.schema import schema
from ponycloud.sparkle.patch import Pointer

__doc__ = """
Database to Dictionary

Collection of classes that proxy our database as a dict-like hierarchy
to be operated on using the JSON Patch utilities.

We define number of helper proxy classes for each level of our virtual
hierarchy.  Namely, the structure looks like this:

    - Children
      `- tables: Collection
                 `- entity_id: Entity
                               `- desired: Desired
                               `- children: Children (recursively)

We start with a Children(parent_table=None, parent_pkey=None) instance
and recurse according to schema and database contents.
For example Collections of Children of a concrete Entity will only
contain children matched by the parent relation.
"""


def validate_dbdict_fragment(schema, fragment, path=[]):
    """
    Wrap selected fragment with several dicts representing the path
    leading to it and then validate it according to given dbdict schema.

    :param schema:    The schema to use for validation.  Use make_schema().
    :param fragment:  The document fragment such as {'uuid': '...', 'foo': 1}.
    :param path:      List of path components such as ['host', 'xy', 'desired'].

    Raise a validation exception if the fragment does not match the schema.
    """

    for part in reversed(path):
        fragment = {part: fragment}

    jsonschema.validate(fragment, schema)


def is_uuid(uuid):
    try:
        return UUID(uuid) and True
    except ValueError:
        return False


def preprocess_dbdict_fragment(fragment, path, uuids, safe):
    """
    Wrap selected fragment with several dicts representing the path
    leading to it and then put it through UUID mapping process.

    Both fragment and path are modified to contain just valid UUIDs.
    Mappings are stored in the uuids dictionary.
    """

    for part in reversed(path):
        fragment = {part: fragment}

    Children.preprocess(fragment, uuids, safe)

    for i in xrange(len(path)):
        key = fragment.keys()[0]
        fragment = fragment[key]
        path[i] = key


def preprocess_dbdict_patch(patch):
    """Preprocess all operations of a valid DbDict patch."""

    uuids = {}

    for op in patch:
        value = op.get('value', {})
        path  = op.get('path', [])

        preprocess_dbdict_fragment(value, path, uuids, ('test' != op['op']))

        if 'from' in op:
            preprocess_dbdict_fragment({}, op['from'], uuids, False)

    return uuids


def make_schema(tenant=None, alicorn=False, write=False):
    """
    Create JSON Schema for specific conditions.

    :param tenant:   Include portions protected by given tenant token.
    :param alicorn:  Superuser access override, include everything.
    :param write:    Hide read-only parts when True.
    """

    def recurse(path=[]):
        result = {
            'type': 'object',
            'additionalProperties': False,
            'properties': {},
        }

        for tname, table in schema.iteritems():
            if 0 == len(path):
                if 0 != len(table['parents']) and not table.get('public'):
                    continue
            else:
                if path[-1] not in [p[1] for p in table['parents']]:
                    continue

            if not alicorn:
                if tname == 'tenant' or 'tenant' in path:
                    if not tenant:
                        continue
                else:
                    if write or not table.get('public'):
                        continue

            if alicorn or tname != 'tenant':
                key = 'patternProperties'
                pattern = '.*'
            else:
                key = 'properties'
                pattern = tenant

            result['properties'][tname] = {
                'type': 'object',
                key: {
                    pattern: {
                        'type': 'object',
                        'additionalProperties': False,
                        'properties': {
                            'desired': {'type': 'object'},
                            'children': recurse(path + [tname]),
                        },
                    },
                },
            }

        return result

    return recurse()


class DbDict(MutableMapping, dict):
    """
    Special container that inherits MutableMapping for it's
    concrete implementations of dict excluding the actual storage.
    We use database as our storage.

    The dict parent is there for isinstance queries performed by the
    jsonpatch library.
    """

    def __getitem__(self, key):
        raise NotImplementedError('__getitem__ not supported')

    def __setitem__(self, key, value):
        raise NotImplementedError('__setitem__ not supported')

    def __delitem__(self, key):
        raise NotImplementedError('__delitem__ not supported')

    def __iter__(self):
        raise NotImplementedError('__iter__ not supported')

    def __contains__(self, key):
        # Probing works much better with a database backend than listing all
        # child keys (imagine listing all tenants) and then scanning them.
        try:
            assert self[key]
            return True
        except KeyError:
            return False

    def __len__(self):
        return len(list(iter(self)))

    def __repr__(self):
        return pformat(self.to_dict())

    def __eq__(self, other):
        if set(self) != set(other):
            return False

        for key in self:
            if self[key] != other[key]:
                return False

        return True

    def to_dict(self):
        """Recursively convert to a plain dictionary."""
        result = {}
        for k, v in self.iteritems():
            if isinstance(v, DbDict):
                result[k] = v.to_dict()
            else:
                result[k] = v

        return result


class Children(DbDict):
    """
    Children are reached from the root or an Entity and represent a
    collection of valid sub-tables for a given entity.

    Immediate contents depend solely on the schema, but the optional
    parent entity is passed to Collection instances it produces.
    """

    def __init__(self, db, parent_table=None, parent_pkey=None):
        """
        :param db:            Reference to the SQLSoup database proxy.
        :param parent_table:  Name of a valid parent table from schema.
        :param parent_pkey:   Parent primary key value to restrict children.
        """
        self.db = db
        self.parent_table = parent_table
        self.parent_pkey = parent_pkey

    def __getitem__(self, key):
        """Retrieve child Collection proxy."""

        if key not in self:
            raise KeyError('%s not a child of %s' % (key, self.parent_table))

        return Collection(self.db, key, self.parent_table, self.parent_pkey)

    def __contains__(self, key):
        """Determine whether the key is a valid child collection."""
        return key in iter(self)

    def add(self, child, value):
        raise TypeError('list of children collections is immutable')

    def __delitem__(self, child):
        raise TypeError('list of children collections is immutable')

    def __iter__(self):
        """Name all tables that have our entity table as their parent."""

        for name, entity in schema.iteritems():
            if self.parent_table is None:
                # When self.parent_table is None, we are interested only in
                # tables that are at the root.
                if len(entity['parents']) == 0:
                    yield name
            else:
                for local_field, remote_table in entity['parents']:
                    if remote_table == self.parent_table:
                        yield name

    @staticmethod
    def preprocess(fragment, uuids, safe):
        for k, v in fragment.iteritems():
            Collection.preprocess(v, uuids, safe)


class Collection(DbDict):
    """Multiple entities of a kind restricted to a particular parent."""

    def __init__(self, db, table, parent_table, parent_pkey):
        self.db = db
        self.table = table
        self.parent_table = parent_table
        self.parent_pkey = parent_pkey

    def __getitem__(self, key):
        """
        Retrieve entity from the collection.
        Attempts to obtain child of a different parent will fail.
        """

        try:
            child = getattr(self.db, self.table).get(key)
        except UnmappedInstanceError:
            raise KeyError('%s/%s not found' % (self.table, key))

        if self.parent_table is None:
            for local_field, remote_table in schema[self.table]['parents']:
                if getattr(child, local_field) is not None:
                    raise KeyError('%s/%s not found' % (self.table, key))
        else:
            fkey = schema.get_fkey(self.table, self.parent_table)
            if getattr(child, fkey) != self.parent_pkey:
                raise KeyError('%s/%s not found' % (self.table, key))

        return Entity(self.db, self.table, key)

    def __iter__(self):
        """Retrieve child keys restricted to collection parent."""

        query = getattr(self.db, self.table)

        if self.parent_table is None:
            for local_field, remote_table in schema[self.table]['parents']:
                query.filter_by(**{local_field: None})
        else:
            fkey = schema.get_fkey(self.table, self.parent_table)
            query.filter_by(**{fkey: self.parent_pkey})

        pkey = schema[self.table]['pkey']
        for row in query.all():
            yield getattr(row, pkey)

    def add(self, key, value):
        """Insert new entity to the collection."""

        desired = dict(value['desired'])

        pkey = schema[self.table]['pkey']

        if pkey in desired and desired[pkey] != key:
            raise KeyError('%s/%s/desired/%s (%s) does not match' \
                            % (self.table, key, pkey, desired[pkey]))

        desired[pkey] = key

        if self.parent_table:
            fkey = schema.get_fkey(self.table, self.parent_table)
            parent_pkey = desired.setdefault(fkey, self.parent_pkey)

            if parent_pkey != self.parent_pkey:
                raise ValueError('cannot add %s/%s with invalid parent %s' \
                                    % (self.table, key, parent_pkey))

        getattr(self.db, self.table).insert(**desired)
        self.db.flush()

    def __delitem__(self, key):
        """Delete child entity by it's primary key."""

        assert self[key]

        try:
            self.db.delete(getattr(self.db, self.table).get(key))
            self.db.flush()
        except UnmappedInstanceError:
            raise KeyError('%s/%s not found' % (self.table, key))

    @staticmethod
    def preprocess(fragment, uuids, safe):
        for k, v in list(fragment.iteritems()):
            if not is_uuid(k):
                if k in uuids:
                    nk = uuids[k]
                else:
                    nk = uuids.setdefault(k, str(uuid4()))

                fragment[nk] = v
                del fragment[k]

            Entity.preprocess(v, uuids, safe)


class Entity(DbDict):
    """Container holding both desired state and children."""

    def __init__(self, db, table, pkey):
        self.db = db
        self.table = table
        self.pkey = pkey

        self.data = {
            'desired': Desired(self.db, self.table, self.pkey),
            'children': Children(self.db, self.table, self.pkey),
        }

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, key):
        try:
            return self.data[key]
        except KeyError:
            raise KeyError('%s/%s/%s not found' % (self.table, self.pkey, key))

    def __delitem__(self, key):
        try:
            self.data[key]
        except KeyError:
            raise KeyError('%s/%s/%s not found' % (self.table, self.pkey, key))

        raise TypeError('cannot remove %s/%s/%s' % (self.table, self.pkey, key))

    def add(self, key, value):
        if key in self.data:
            raise KeyError('%s/%s/%s already exists' \
                                % (self.table, self.pkey, key))

        raise TypeError('%s/%s/%s cannot exist' % (self.table, self.pkey, key))

    @staticmethod
    def preprocess(fragment, uuids, safe):
        for k, v in fragment.iteritems():
            if 'desired' == k:
                Desired.preprocess(v, uuids, safe)
            elif 'children' == k:
                Children.preprocess(v, uuids, safe)


class Desired(DbDict):
    """
    Implementation of the 'desired' key of an Entity dict.
    Provides access to columns of the database row.
    """

    def __init__(self, db, table, pkey):
        self.db = db
        self.table = table
        self.pkey = pkey

    def get_soup_table(self):
        return getattr(self.db, self.table)

    def get_soup_entity(self):
        return self.get_soup_table().get(self.pkey)

    def __iter__(self):
        """List database columns with non-null values."""

        entity = self.get_soup_entity()

        for key in self.get_soup_table().c.keys():
            # We treat keys with NULL values as undefined.
            if getattr(entity, key) is not None:
                yield key

    def __contains__(self, key):
        return key in iter(self)

    def __getitem__(self, key):
        """Retrieve value of a non-NULL key."""

        value = getattr(self.get_soup_entity(), key, None)

        # When the result is non-None the key still might be something
        # unexpected such as a builtin method.  So check key validity too.
        if value is None or key not in self:
            raise KeyError('%s/%s/desired/%s not found' \
                                % (self.table, self.pkey, key))

        return value

    def __delitem__(self, key):
        """
        Deleting is not really possible, but we can always attempt to set
        the value to NULL.  We only have to be careful as not to modify
        primary key because they should be immutable.
        """

        if key == schema[self.table]['pkey']:
            raise TypeError('%s/%s/desired/%s is immutable primary key' \
                                % (self.table, self.pkey, key))

        entity = self.get_soup_entity()

        if getattr(entity, key, None) is None or key not in self:
            raise KeyError('%s/%s/desired/%s not found' \
                                % (self.table, self.pkey, key))

        for local_field, remote_table in schema[self.table]['parents']:
            if local_field == key:
                raise KeyError('%s/%s/desired/%s is immutable' \
                                    % (self.table, self.pkey, key))

        setattr(entity, key, None)
        self.db.flush()

    def add(self, key, value):
        """Set previously NULL field."""

        if key in self:
            raise KeyError('%s/%s/desired/%s already exists' \
                                % (self.table, self.pkey, key))

        for local_field, remote_table in schema[self.table]['parents']:
            if local_field == key:
                raise KeyError('%s/%s/desired/%s is immutable' \
                                    % (self.table, self.pkey, key))

        entity = self.get_soup_entity()
        setattr(entity, key, value)
        self.db.flush()

    def replace(self, key, value):
        """Change value of an existing field."""

        if value is None:
            raise ValueError('%s/%s/desired/%s cannot be null' \
                                % (self.table, self.pkey, key))

        if key not in self:
            raise KeyError('%s/%s/desired/%s not found' \
                                % (self.table, self.pkey, key))

        for local_field, remote_table in schema[self.table]['parents']:
            if local_field == key:
                raise KeyError('%s/%s/desired/%s is immutable' \
                                    % (self.table, self.pkey, key))

        entity = self.get_soup_entity()
        setattr(entity, key, value)
        self.db.flush()

    @staticmethod
    def preprocess(fragment, uuids, safe):
        for k, v in fragment.iteritems():
            if 'uuid' == k:
                if is_uuid(v):
                    if safe:
                        raise ValueError('user-defined uuid %r' % (v,))
                else:
                    if v in uuids:
                        nv = uuids[v]
                    else:
                        nv = uuids.setdefault(v, str(uuid4()))

                    fragment[k] = nv


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
