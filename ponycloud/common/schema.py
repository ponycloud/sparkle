#!/usr/bin/python -tt

import yaml
import os.path
import jsonschema

__all__ = ['Schema', 'schema']


class Schema(dict):
    def get_fkey(self, local_table, remote_table):
        """
        Retrieve foreign key pointing from local_table to remote_table.
        """

        assert local_table in self,  'unknown table %r' % (local_table,)
        assert remote_table in self, 'unknown table %r' % (remote_table,)

        for local_column, parent_table in self[local_table]['parents']:
            if parent_table == remote_table:
                return local_column

        raise KeyError('no foreign key relation of %s to %s' \
                        % (local_table, remote_table))

    def iter_paths(self, prefix=()):
        """
        Iterate over all possible paths from parent/child relations.
        Paths are returned as tuples of entity names.

        For example::

            iter([
                ('cpu_profile'),
                ('cpu_profile', 'instance'),
                ...
            ])
        """

        if 0 == len(prefix):
            for tname, table in schema.iteritems():
                if 0 == len(table['parents']):
                    for sub in self.iter_paths((tname,)):
                        yield sub
        else:
            yield prefix
            for tname, table in schema.iteritems():
                if prefix[-1] in [rt for lf, rt in table['parents']]:
                    for sub in self.iter_paths(prefix + (tname,)):
                        yield sub


def load_schema():
    with open(os.path.dirname(__file__) + '/schema.schema.yaml') as fp:
        schema_schema = yaml.load(fp)

    with open(os.path.dirname(__file__) + '/schema.yaml') as fp:
        schema = yaml.load(fp)

    jsonschema.validate(schema, schema_schema)
    return Schema(schema)


schema = load_schema()


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
