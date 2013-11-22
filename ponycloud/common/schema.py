#!/usr/bin/python -tt

import yaml
import os.path
import jsonschema

__all__ = ['Schema', 'schema']


class Schema(dict):
    def get_fkey(self, local_table, remote_table):
        for local_column, parent_table in self[table_name]['parents']:
            if parent_table == remote_table:
                return local_column

        return None


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
