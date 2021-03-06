#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = []

from psycopg2.extensions import *
from psycopg2.extras import NumericRange
from sqlalchemy.types import UserDefinedType
from sqlalchemy.dialects.postgresql.base import ischema_names
from simplejson import loads, dumps


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


def cast_json(value, cur):
    """A json->dict converter to process values coming from psycopg2."""
    if value is None:
        return None
    return loads(value)

# Make JSON types.
JSON = new_type((114,), "JSON", cast_json)
JSONARRAY = new_array_type((199,), 'JSONARRAY', JSON)

# Register them.
register_type(JSON)
register_type(JSONARRAY)


def adapt_dict(value):
    """Converts dict passed to psycopg2 to JSON string."""
    return QuotedString(dumps(value))

# Register dict->json adapter.
register_adapter(dict, adapt_dict)

# Monkey-patch the numeric range to be JSON-serializable.
# Will explode if the range is not `[)` without infinity.
NumericRange.for_json = lambda self: [self.lower, self.upper]


# vim:set sw=4 ts=4 et:
