#!/usr/bin/python -tt

__all__ = []

from psycopg2.extensions import *
from sqlalchemy.types import UserDefinedType
from sqlalchemy.dialects.postgresql.base import ischema_names

import cjson

def make_transparent_type(name):
    class TransparentType(UserDefinedType):
        def get_col_spec(self):
            return name

        def bind_processor(self, dialect):
            def process(value):
                return value
            return process

        def result_processor(self, dialect, coltype):
            def process(value):
                return value
            return process
    return TransparentType

ischema_names['json'] = make_transparent_type('JSON')
ischema_names['int8range'] = make_transparent_type('INT8RANGE')

# Register inet as string type.
register_type(new_type((869,), 'INET', UNICODE))
register_type(new_array_type((1041,), 'INETARRAY', UNICODE))

# A json->dict converter to process values coming from psycopg2.
def cast_json(value, cur):
    if value is None:
        return None
    return cjson.decode(value)

# Make JSON types.
JSON = new_type((114,), "JSON", cast_json)
JSONARRAY = new_array_type((199,), 'JSONARRAY', JSON)

# Register them.
register_type(JSON)
register_type(JSONARRAY)

# Converts dict passed to psycopg2 to JSON string.
def adapt_dict(value):
    return QuotedString(cjson.encode(value))

# Register dict->json adapter.
register_adapter(dict, adapt_dict)


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
