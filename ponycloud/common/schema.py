#!/usr/bin/python -tt

import yaml
import os.path

__all__ = ['Schema', 'schema']


class Root(object):
    def __init__(self):
        self.table  = None
        self.access = 'private'
        self.filter = {}
        self.parent = None
        self.children = {}

    @property
    def public(self):
        return {name: child.public \
                for name, child in self.children.iteritems() \
                if child.access not in ('private',)}


class Endpoint(object):
    def __init__(self, table, path, mount):
        self.table  = table
        self.access = mount['access']
        self.filter = mount.get('filter', {})
        self.parent = None
        self.children = {}

    @property
    def public(self):
        return {
            'pkey': self.table.pkey,
            'fkeys': list(self.table.fkeys),
            'table': self.table.name,
            'access': self.access,
            'children': {name: child.public \
                         for name, child in self.children.iteritems() \
                         if child.access not in ('private',)}
        }


class Table(object):
    def __init__(self, tname, table):
        self.name  = tname
        self.pkey  = table['pkey']
        self.index = set(table.get('index', []))
        self.fkeys = set()
        self.virtual = table.get('virtual', False)
        self.endpoints = {}

        if isinstance(self.pkey, basestring):
            self.index.add(self.pkey)
        else:
            for key in self.pkey:
                self.index.add(key)


class Schema(object):
    def __init__(self, data):
        self.root = Root()
        self.endpoints = {(): self.root}
        self.tables = {}

        for tname, table in data.iteritems():
            self.tables[tname] = Table(tname, table)

        for tname, info in data.iteritems():
            table = self.tables[tname]
            for strpath, mount in info['mount'].iteritems():
                if mount['access'] != 'private':
                    path = tuple(strpath.split('/')[1:])
                    self.endpoints[path] = Endpoint(table, path, mount)

        for path, mount in self.endpoints.iteritems():
            if len(path) > 0:
                mount.table.endpoints[path] = mount
                mount.parent = self.endpoints[path[:-1]]
                mount.parent.children[path[-1]] = mount

            if mount.parent is not None and mount.parent.table is not None:
                mount.table.index.add(mount.parent.table.name)
                mount.table.fkeys.add(mount.parent.table.name)

        del self.endpoints[()]


def load_schema():
    with open(os.path.dirname(__file__) + '/schema.yaml') as fp:
        schema = yaml.load(fp)

    return Schema(schema)


schema = load_schema()


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
