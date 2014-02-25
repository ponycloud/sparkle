#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import re

from collections import Mapping, MutableMapping, Sequence, MutableSequence
from sqlalchemy.orm.exc import UnmappedInstanceError
from pprint import pformat
from uuid import uuid4

from sparkle.common import *
from sparkle.schema import schema

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

We start with a Children(db, schema.root, pkey=None) instance
and recurse according to schema and database contents.
For example Collections of Children of a concrete Entity will only
contain children matched by the parent relation.
"""


def is_uuid(uuid):
    if not isinstance(uuid, basestring):
        return False

    try:
        return re.match('[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}', uuid) and True
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

    Children.preprocess(fragment, uuids, schema.root, safe)

    for i in xrange(len(path)):
        key = fragment.keys()[0]
        fragment = fragment[key]
        path[i] = key

    return fragment


def preprocess_dbdict_patch(patch):
    """Preprocess all operations of a valid DbDict patch."""

    uuids = {}

    for op in patch:
        value = op.get('value', {})
        path  = op.get('path', [])
        write = ('test' != op['op'])

        try:
            op['value'] = preprocess_dbdict_fragment(value, path, uuids, write)

            if 'from' in op:
                preprocess_dbdict_fragment({}, op['from'], uuids, False)
        except ValueError, e:
            raise PatchError(e.message, path)

    return uuids


def make_schema(credentials, write=False):
    """
    Create JSON Schema for specific conditions.

    :param credentials: Access credentials that can contain either
                        authorization for a particular tenant,
                        particular user, whole system or nothing.

    :param write:       Toggles between write-only and read-only JSON schema.

    :returns: Schema
    """

    alicorn = credentials.get('alicorn', False)
    tenant  = credentials.get('tenant')
    role    = credentials.get('role')
    user    = credentials.get('user')

    def recurse(endpoint):
        result = {
            'type': 'object',
            'additionalProperties': False,
            'properties': {},
        }

        for name, child in endpoint.children.iteritems():
            # Include all rows if allowed access to that table by default.
            key = 'patternProperties'
            pattern = '.*'

            if alicorn:
                # Alicorns override all access control.
                pass

            else:
                if child.access == 'protected':
                    # Alicorns-only endpoints.
                    continue

                if write and child.access == 'shared':
                    # Alicorns-write others-read endpoints.
                    continue

                if child.table.name == 'tenant':
                    # Special case for tenant isolation.
                    # Needs correct tenant credentials for access.
                    if tenant is None:
                        continue
                    key = 'properties'
                    pattern = tenant

                if child.access == 'tenant/owner' and role != 'owner':
                    # Endpoints limited to tenant owners.
                    continue

                if child.access == 'tenant/user' and role == 'operator':
                    # Operators cannot write to tenant data.
                    if write:
                        continue

                if child.table.name == 'user':
                    # Special case for user isolation.
                    # Needs correct user credentials for access.
                    if user is None:
                        continue
                    key = 'properties'
                    pattern = user

                if child.access == 'user/rw' and user is None:
                    # When valid user is required for access
                    continue

                if child.access == 'user/ro' and user is None:
                    # When valid user is required for read-only access
                    # and all writes are forbidden.
                    if write:
                        continue

            result['properties'][name] = {
                'type': 'object',
                key: {
                    pattern: {
                        'type': 'object',
                        'additionalProperties': False,
                        'properties': {
                            'desired': {'type': 'object'},
                            'children': recurse(child),
                        },
                    },
                },
            }

        return result

    return recurse(schema.root)


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

    def __init__(self, db, schema, pkey=None):
        """
        :param db:     Reference to the SQLSoup database proxy.
        :param schema: Parent endpoint schema information.
        :param pkey:   Parent primary key value to restrict children.
        """
        self.db = db
        self.schema = schema
        self.pkey = pkey

    def __getitem__(self, key):
        """Retrieve child Collection proxy."""

        if key not in self:
            raise KeyError(key)

        return Collection(self.db, self.schema.children[key], self.pkey)

    def __contains__(self, key):
        """Determine whether the key is a valid child collection."""
        return key in iter(self)

    def add(self, child, value):
        raise TypeError('list of children collections is immutable')

    def __delitem__(self, child):
        raise TypeError('list of children collections is immutable')

    def __iter__(self):
        """Iterate over names of all non-virtual child endpoints."""

        for name, child in self.schema.children.iteritems():
            if not child.table.virtual:
                yield name

    @staticmethod
    def preprocess(fragment, uuids, cschema, safe):
        for k, v in fragment.iteritems():
            Collection.preprocess(v, uuids, cschema.children[k], safe)


class Collection(DbDict):
    """Multiple entities of a kind restricted to a particular parent."""

    def __init__(self, db, schema, pkey):
        self.db = db
        self.schema = schema
        self.pkey = pkey

    def __getitem__(self, key):
        """
        Retrieve entity from the collection.
        Attempts to obtain child of a different parent will fail.
        """

        try:
            # Load the child entity for additional checking.
            child = getattr(self.db, self.schema.table.name).get(key)
        except UnmappedInstanceError:
            raise KeyError(key)

        # Verify that all filters are met.
        for field, value in self.schema.filter.iteritems():
            if getattr(child, field) != value:
                raise KeyError(key)

        # If not at the root, verify that this entity belongs to
        # parent it have been loaded from.
        if self.schema.parent.table is not None:
            fkey = self.schema.parent.table.name
            if getattr(child, fkey) != self.pkey:
                raise KeyError(key)

        # The entity provably belongs to this parent, return it.
        return Entity(self.db, self.schema, key)

    def __iter__(self):
        """Retrieve child keys restricted to collection parent."""

        # Prepare simple query on this table.
        query = getattr(self.db, self.schema.table.name)

        # Apply filters from schema.
        query.filter_by(**self.schema.filter)

        # If we have a parent, relate to it using a foreign key column
        # that is by convention called same as the parent table.
        if self.schema.parent.table is not None:
            query.filter_by(**{self.schema.table.name: self.pkey})

        # Return primary keys of all queried rows.
        for row in query.all():
            yield getattr(row, self.schema.pkey)

    def add(self, key, value):
        """Insert new entity to the collection."""

        desired = dict(value.get('desired', {}))
        pkey = self.schema.table.pkey

        if pkey in desired and desired[pkey] != key:
            raise ValueError('mismatched primary key')

        desired[pkey] = key

        if self.schema.parent.table is not None:
            fkey = self.schema.parent.table.name
            parent_pkey = desired.setdefault(fkey, self.pkey)

            if parent_pkey != self.pkey:
                raise ValueError('invalid parent')

        getattr(self.db, self.schema.table.name).insert(**desired)
        self.db.flush()

        # Recurse into children collections.
        for name, children in value.get('children', {}).iteritems():
            for k, v in children.iteritems():
                self[key]['children'][name].add(k, v)

    def __delitem__(self, key):
        """Delete child entity by it's primary key."""

        if key not in self:
            raise KeyError(key)

        try:
            self.db.delete(getattr(self.db, self.schema.table.name).get(key))
            self.db.flush()
        except UnmappedInstanceError:
            raise KeyError(key)

    @staticmethod
    def preprocess(fragment, uuids, cschema, safe):
        for k, v in fragment.items():
            # Only enforce on uuid or composite keys.
            if cschema.table.pkey == 'uuid' or \
               not isinstance(cschema.table.pkey, basestring) \
               and not cschema.table.user_pkey:

                # If the key is not a valid uuid, treat it as a placeholder
                # and generate new replacement uuid.
                if not is_uuid(k):
                    if k in uuids:
                        nk = uuids[k]
                    else:
                        nk = uuids.setdefault(k, str(uuid4()))

                    fragment[nk] = v
                    del fragment[k]
                    k = nk

            Entity.preprocess(fragment[k], uuids, cschema, safe)


class Entity(DbDict):
    """Container holding both desired state and children."""

    def __init__(self, db, schema, pkey):
        self.db = db
        self.schema = schema
        self.pkey = pkey

        self.data = {
            'desired': Desired(db, schema, pkey),
            'children': Children(db, schema, pkey),
        }

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, key):
        try:
            return self.data[key]
        except KeyError:
            raise KeyError(key)

    def __delitem__(self, key):
        if key not in self.data:
            raise KeyError(key)

        raise TypeError('immutable field')

    def add(self, key, value):
        if key in self.data:
            raise KeyError(key)

        raise TypeError('immutable field')

    @staticmethod
    def preprocess(fragment, uuids, eschema, safe):
        for k, v in fragment.iteritems():
            if 'desired' == k:
                Desired.preprocess(v, uuids, eschema, safe)
            elif 'children' == k:
                Children.preprocess(v, uuids, eschema, safe)


class Desired(DbDict):
    """
    Implementation of the 'desired' key of an Entity dict.
    Provides access to columns of the database row.
    """

    def __init__(self, db, schema, pkey):
        self.db = db
        self.schema = schema
        self.pkey = pkey

    def get_soup_table(self):
        return getattr(self.db, self.schema.table.name)

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
            raise KeyError(key)

        return value

    def __delitem__(self, key):
        """
        Deleting is not really possible, but we can always attempt to set
        the value to NULL.  We only have to be careful as not to modify
        primary key because they should be immutable.
        """

        if key == self.schema.table.pkey:
            raise TypeError('immutable primary key')

        entity = self.get_soup_entity()

        if getattr(entity, key, None) is None or key not in self:
            raise KeyError(key)

        if self.schema.parent.table and key == self.schema.parent.table.name:
            raise KeyError(key)

        setattr(entity, key, None)
        self.db.flush()

    def add(self, key, value):
        """Set previously NULL field."""

        if key in self:
            raise ValueError('already exists')

        if self.schema.parent.table and key == self.schema.parent.table.name:
            raise TypeError('immutable field')

        entity = self.get_soup_entity()
        setattr(entity, key, value)
        self.db.flush()

    def replace(self, key, value):
        """Change value of an existing field."""

        if value is None:
            raise ValueError('cannot be null')

        if key not in self:
            raise KeyError(key)

        if self.schema.parent.table and key == self.schema.parent.table.name:
            raise ValueError('immutable field')

        entity = self.get_soup_entity()
        setattr(entity, key, value)
        self.db.flush()

    @staticmethod
    def preprocess(fragment, uuids, dschema, safe):
        # Generate set of keys that should be uuids.
        # Start with a possibly 'uuid' primary key.
        uuid_pkeys = set(['uuid'])
        uuid_fkeys = set()

        # Except when we explicitly want user-defined uuids.
        if dschema.table.user_pkey:
            uuid_pkeys = set()

        # Include all composite primary key fields if applicable.
        if not isinstance(dschema.table.pkey, basestring):
            for pkey in dschema.table.pkey:
                uuid_pkeys.add(pkey)

        # And definitely add all foreign keys that have an 'uuid'
        # primary key as their target.
        for fkey in dschema.table.fkeys:
            ftable = schema.tables[fkey]
            if ftable.pkey == 'uuid' and not ftable.user_pkey:
                uuid_fkeys.add(fkey)

        # Adjust fields as needed.
        for k, v in fragment.iteritems():
            if k in uuid_pkeys or k in uuid_fkeys:
                if is_uuid(v):
                    if k in uuid_pkeys and safe:
                        # Primary keys cannot be set by the user.
                        raise ValueError('user-defined pkey uuid %r' % (v,))
                else:
                    try:
                        if v in uuids:
                            nv = uuids[v]
                        else:
                            nv = uuids.setdefault(v, str(uuid4()))
                    except TypeError:
                        # Ignore problems with unhashable data from user,
                        # we will fail later in a more meaningful way.
                        continue

                    fragment[k] = nv


# vim:set sw=4 ts=4 et:
