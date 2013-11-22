#!/usr/bin/python -tt

import jsonpatch
import re

from sqlalchemy.exc import DataError
from sqlalchemy.orm.exc import UnmappedInstanceError
from schema import schema
from pprint import pformat
from uuid import uuid4
from collections import MutableMapping


class DbDict(MutableMapping, dict):
    """
    Basically a dict with some magic.
    """

    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def keys(self):
        return self.store.keys()

    def values(self):
        return self.store.values()

    def iteritems(self):
        return self.__iter__()

    def __iter__(self):
        return iter(self.store)

    def __repr__(self):
        return pformat(self.to_dict())

    def __len__(self):
        return len(self.store)

    def to_dict(self):
        """ This is called when we want to dump the structure """
        val = {}
        for i in self:
            if isinstance(self[i], DbDict):
                val[i] = self[i].to_dict()
            else:
                val[i] = self[i]
        return val


class Root(DbDict):
    """
    Root element of this monster.

    JsonPath for this structure should be in following format:
    {root}/{collection}/{entity}/{desired, children} and then back to collection
    """

    def __init__(self, db):
        self.db = db

    def __getitem__(self, key):
        return Collection(self, key)

    def get_table(self, name):
        return self.db.__getattr__(name)

    def __setitem__(self, key, value):
        """
        Pass to collection
        """
        collection = Collection(self, key)
        for pkey in value:
            collection[pkey] = value[pkey]

    def commit(self):
        return self.db.commit()

    def rollback(self):
        return self.db.rollback()

    def __iter__(self):
        return iter({k: self[k] for k in schema})

    def keys(self):
        return [k for k in schema]

    def values(self):
        return [self[k] for k in schema]


class Desired(DbDict):
    """
    This is the desired state, it contains db columns of given entity and appropriate values
    """
    def __init__(self, entity):
        """
        :param entity: Mapped entity from SQLSoup
        """
        self.entity = entity

    def __getitem__(self, key):
        return self.entity.__getattribute__(key)

    def __setitem__(self, key, value):
        #print 'Setting in entity dict %s to %s' % (key, value)
        setattr(self.entity, key, value)

    def __delitem__(self, key):
        """
        This should not happen. It would mean deleting the item, however we cannot do this
        and we cannot raise exception here because of json patch replace operation.
        """
        pass

    def __iter__(self):
        return iter(self.entity.c.keys())

    def to_dict(self):
        cols = self.entity.c.keys()
        data = dict(zip(cols, [self.entity.__getattribute__(c) for c in cols]))
        return data


class Entity(DbDict):
    """
    The entity itself. This entity has Desired state and Children
    """
    def __init__(self, root, table_name, key, parent=None):
        """
        :param root: root element of this tree
        :type root: Root
        :param table_name: name of the table
        :type table_name: str
        :param key: primary key of the Entity (usually uuid)
        :type key: str
        :param parent: parent element to this Entity
        :type parent: Entity
        """

        self.root = root
        self.table_name = table_name
        # table object
        self.table = self.root.get_table(table_name)
        # schema of this table
        self.schema = schema[table_name]
        # parent element
        self.parent = parent

        # If true, then allow updates of the entire entity
        self.allow_update = False

        # Primary key of this entity
        self.key = key

        super(Entity, self).__init__()

        if not key:
            self.db_entity = None
        else:
            try:
                self.db_entity = self.table.get(key)
            except DataError:
                self.db_entity = None

    def __repr__(self):
        return '<Entity %s:%s>' % (self.table_name, self.key)

    def __getitem__(self, key):
        if key == 'desired':
            if self.db_entity is None:
                desired = {k: '' for k in self.table.c.keys()}
            else:
                desired = Desired(self.db_entity)
            return desired
        elif key == 'children':
            return self._get_children()
        else:
            raise KeyError('Entity keys are "desired" or "children"')

    def __setitem__(self, key, value):
        #print 'Setting item in entity %s, %s' % (key, value)
        if key == 'desired':
            # Correct the key
            if not self.key:
                self.key = value[self.schema['pkey']]
            else:
                value[self.schema['pkey']] = self.key

            if self.db_entity is None:
                # We're inserting
                if self.parent is not None:
                    # fixing foreign keys
                    fkey = schema.get_fkey(self.table_name, self.parent.table_name)
                    value[fkey] = self.parent['desired'][self.parent.schema['pkey']]
                self.create_desired(value)
            else:
                # We're updating
                if self.allow_update:
                    desired = Desired(self.table.get(self.key))
                    for col in value:
                        desired[col] = value[col]
                else:
                    raise Exception('Cannot update entire entity')

        if key == 'children':
            # Pass the values to children
            children = Children(self.root, self.table_name, self.key)
            for child_table in value:
                children[child_table] = value[child_table]

    def create_desired(self, entity_dict):
        #print 'INSERTING %s' % entity_dict
        return Desired(self.table.insert(**entity_dict))

    def __delitem__(self, key):
        # hack to make both ways possible, you can either delete the whole entity or its desired
        if key == 'desired':
            key = self.key

        #print 'Deleting from <Entity:%s> with key %s' % (self.table_name, key)
        try:
            self.root.db.delete(self.table.get(key))
            self.root.db.flush()
        except UnmappedInstanceError:
            raise KeyError('<Entity:%s> does not contain key %s' % (self.table_name, key))

    def _get_children(self):
        children = {}
        if 'children' in self.schema and self.schema['children'] is not None:
            children = Children(self.root, self.table_name, self.key)
        return children

    def to_dict(self):
        return {'desired': self['desired'],
                'children': self['children']}


class Collection(DbDict):
    """
    Collection is representing multiple entities
    of a particular kind (eg. instances).
    In terms of a path, it's /top_level/[collection]/entity_identifier/...
    """
    def __init__(self, root, table_name):
        self.root = root
        # table object
        self.table = self.root.get_table(table_name)
        # table name
        self.table_name = table_name
        # schema of this table
        self.schema = schema[table_name]

    def __getitem__(self, key):
        return Entity(self.root, self.table_name, key)

    def keys(self):
        return [item.__getattribute__(self.schema['pkey']) for item in self.table.all()]

    def values(self):
        return [self[item.__getattribute__(self.schema['pkey'])] for item in self.table.all()]

    def __iter__(self):
        return iter({item.__getattribute__(self.schema['pkey']): self[item.__getattribute__(self.schema['pkey'])]
                     for item in self.table.all()})

    def __len__(self):
        return len([item.__getattribute__(self.schema['pkey']) for item in self.table.all()])

    def __delitem__(self, key):
        """
        :param key: value of primary primary key of the represented table
        """
        #print 'Deleting from <Collection:%s> with key %s' % (self.table_name, key)
        try:
            self.root.db.delete(self.table.get(key))
            self.root.db.flush()
        except UnmappedInstanceError:
            raise KeyError('<Collection:%s> does not contain key %s' % (self.table_name, key))

    def __setitem__(self, key, value):
        """
        :param key: primary key value of the entity
        :param value: something like {'desired': {'col': 'val'},
                                      'children': {...}}
        """
        #print 'Setting in collection %s to %s' % (key, value)
        entity = self[key]
        for part in value:  # passing desired and children to entity
            entity[part] = value[part]


class Children(DbDict):
    """
    Children is part of Entity, it stores it's values to a little ambiguous ChildrenStore.
    """
    def __init__(self, root, parent, key):
        """
        :param root: instance of Root
        :param parent: parent table name (eg. 'instance')
        :param key: parent joining key value (eg. some uuid)
        """

        self.store = ChildrenStore()
        self.root = root
        self.parent = parent
        self.schema = schema[parent]['children']
        self.parent_entity = Entity(root, parent, key)

        for child in self.schema:
            parent_col, table_name = child
            # store for listing
            self.store[table_name] = ChildrenStore(root=self.root, table_name=table_name, parent=self.parent_entity)
            # table object for serious business
            child_table = self.root.get_table(table_name)
            # get related children
            for entity in child_table.filter_by(**{parent_col: key}).all():
                pkey = Desired(entity)[schema[table_name]['pkey']]
                child = Collection(self.root, table_name)[pkey]

                if child.parent is None:
                    child.parent = self.parent_entity
                self.store[table_name][pkey] = child

    def __getitem__(self, table_name):
        return self.store[table_name]

    def __setitem__(self, table_name, value):
        """
        :param table_name: name of the child table
        :param value: dict such as {'1234-5678-in-uuid-format': {'desired': {'column1': 'value1', ...}}, ... ,
                                    'another-uuid': {'desired': {...}, 'children': {...}}}
        """
        #print 'Setting in children %s, %s' % (table_name, value)
        entity = self.store[table_name]
        for pkey in value:
            if pkey in entity:
                # This means update
                #print 'Updating in children %s, %s' % (table_name, value)
                entity[pkey][pkey] = value[pkey]
            else:
                #print 'Inserting into children %s, %s' % (table_name, value)
                entity = Entity(self.root, table_name, pkey, self.parent_entity)
                for part in value[pkey]:
                    entity[part] = value[pkey][part]


class ChildrenStore(DbDict):
    """
    This is a the part of a path where Desired and Children dicts reside
    """
    def __init__(self, root=None, table_name=None, parent=None, *args, **kwargs):
        self.root = root
        self.table_name = table_name
        self.parent = parent
        self.store = {}

    def __delitem__(self, key):
        #print 'Deleting %s from table %s in EntityTuple' % (key, self.table_name)
        try:
            entity = self.root.db.__getattr__(self.table_name)
            self.root.db.delete(entity.get(key))
            self.root.db.flush()
            del self.store[key]
        except UnmappedInstanceError:
            raise KeyError('<EntityTuple:%s> does not contain %s' % (self.table_name, key))

    def __setitem__(self, key, value):
        self.store[key] = value
        #print 'Setting in <EntityTuple:%s> %s:%s' % (self.table_name, key, value)
        if self.parent is not None:
            #print 'Out parent is %s' % self.parent
            entity = Entity(self.root, self.table_name, key, self.parent)
            for part in value:
                entity[part] = value[part]


class Preprocessor:
    """
    This class is responsible for generating uuids
    """
    def __init__(self):
        self.matched = {}

    def place_uuid(self, input_str):
        if isinstance(input_str, list):
            return map(self.place_uuid, input_str)
        elif isinstance(input_str, dict):
            for key in input_str:
                if isinstance(input_str[key], list):
                    input_str[key] = self.place_uuid(input_str[key])
                else:
                    input_str[key] = self.place_uuid(input_str[key])
                new_key = self.place_uuid(key)
                if new_key != key:
                    input_str[new_key] = input_str[key]
                    del (input_str[key])

        else:
            pattern = '%uuid\[([0-9]+)\]%'
            return re.sub(pattern, self.get_uuid, str(input_str))
        return input_str

    def get_uuid(self, matched):
        key = matched.group(1)
        if key not in self.matched:
            self.matched[key] = str(uuid4())
        return self.matched[key]


def apply_patch(db, patch):
    desired = Root(db)
    prep = Preprocessor()
    patch = prep.place_uuid(patch)
    jsonpatch.apply_patch(desired, patch, True)
    return desired.commit()


if __name__ == '__main__':

    hnus = [{'op': 'add', 'path': '/instance/%uuid[5]%',
            'value': {'desired':
                          {
                           'tenant': '76764219-6bd4-4278-8b7b-659fc43c939e',
                           'name': 'Precious Instance %uuid[5]%',
                           'state': 'running',
                           'vcpu': 1,
                           'rcpu': 0.1,
                           'mem': 1024,
                           'cpu_profile': '681a3fc6-313f-4772-87f4-595b53bb20af',
                           'ns': ['8.8.8.8']
                          },
                      'children': {
                          'vdisk': {
                              '%uuid[7]%': {'desired':
                                                {'uuid': '%uuid[7]%',
                                                 'instance': '%uuid[5]%',
                                                 'volume': '4d6d6ca4-eb09-4d09-8c47-c7918d672e8e',
                                                 'storage_pool': '2d8ba590-4b1f-4512-87db-f6dd44155f01',
                                                 'index': 1,
                                                 'size': 1024}},
                              '%uuid[8]%': {'desired':
                                                {'uuid': '%uuid[8]%',
                                                 'instance': '%uuid[5]%',
                                                 'volume': '4d6d6ca4-eb09-4d09-8c47-c7918d672e8e',
                                                 'storage_pool': '2d8ba590-4b1f-4512-87db-f6dd44155f01',
                                                 'index': 2,
                                                 'size': 4096}}
                          }
                      }
            }},
            {'op': 'replace', 'path': '/instance/%uuid[5]%', 'value': {'desired': {'name': 'My Precious Instance Updated'}}},
            {'op': 'remove', 'path': '/instance/%uuid[5]%/children/vdisk/%uuid[7]%'}
            ]

    flus = [{'op': 'add', 'path': '/instance/%uuid[1]%',
            'value': {'desired':
                          {
                           'tenant': '76764219-6bd4-4278-8b7b-659fc43c939e',
                           'name': 'Precious Instance %uuid[5]%',
                           'state': 'running',
                           'vcpu': 1,
                           'rcpu': 0.1,
                           'mem': 1024,
                           'cpu_profile': '681a3fc6-313f-4772-87f4-595b53bb20af',
                           'ns': ['8.8.8.8']
                          },
                      'children': {
                          'vdisk': {
                              '%uuid[2]%': {'desired':
                                                {'volume': '4d6d6ca4-eb09-4d09-8c47-c7918d672e8e',
                                                 'storage_pool': '2d8ba590-4b1f-4512-87db-f6dd44155f01',
                                                 'index': 2,
                                                 'size': 2048}}
                          }
                      }
            }},
            {'op': 'replace', 'path': '/instance/%uuid[1]%/desired/name', 'value': 'Whatever %uuid[10]%'},
            {'op': 'replace',
             'path': '/instance/%uuid[1]%/children/vdisk/%uuid[2]%',
             'value': {
                 'desired': {
                     'volume': '4d6d6ca4-eb09-4d09-8c47-c7918d672e8e',
                     'storage_pool': '2d8ba590-4b1f-4512-87db-f6dd44155f01',
                     'index': 1,
                     'size': 5120}
             }
            },
                    #{'op': 'remove', 'path': '/instance/%uuid[1]%'}
    ]


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
