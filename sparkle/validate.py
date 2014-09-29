#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

__doc__ = """
Validator Functions

Routines that help with validation of various data structures the API
needs to make sense of.
"""

__all__ = ['validate_json_patch', 'validate_dbdict',
           'validate_dbdict_fragment']

from sparkle.common import *
from sparkle.schema import schema

from collections import Mapping

import jsonschema
import os.path
import yaml


# Load the JSON Patch schema specification from an outside file.
with open(os.path.dirname(__file__) + '/patch.schema.yaml') as fp:
    JSON_PATCH_SCHEMA = yaml.load(fp)


def validate_json_patch(data):
    """
    Validate that given object corresponds to a valid JSON Patch.

    We tolerate usage of list instead of string for path and from properties,
    accept additional operations 'x-merge' and 'x-verify', but otherwise
    don't peek into the patch values.

    Raises an DataError if the patch does not match the prescribed schema.
    """

    try:
        jsonschema.validate(data, JSON_PATCH_SCHEMA)
    except jsonschema.ValidationError, e:
        raise PatchError(e.message, list(e.path))


def validate_dbdict(creds, data, write):
    """
    Validate given database->dictionary mapping with respect
    to supplied credentials and access mode (read/write).

    The data are assumed to start at the schema root.

    Raises an appropriate UserError.
    """

    alicorn = creds.get('alicorn', False)
    tenant = creds.get('tenant')
    role = creds.get('role')
    user = creds.get('user')

    def check_desired(data, ep, path):
        if not isinstance(data, Mapping):
            raise DataError('invalid desired state', path)

        for key in data:
            if not isinstance(key, basestring):
                raise DataError('invalid field name', path + [key])

    def check_children(data, ep, path):
        if not isinstance(data, Mapping):
            raise DataError('invalid children', path)

        for key, collection in data.iteritems():
            if not isinstance(key, basestring):
                raise DataError('invalid key type', path + [key])

            if key not in ep.children:
                raise DataError('invalid child type', path + [key])

            check_collection(collection, ep.children[key], path + [key])

    def check_entity(data, ep, path):
        if not isinstance(data, Mapping):
            raise DataError('invalid entity', path)

        auth_entity(ep, path)

        for key in data:
            if key not in ('desired', 'children'):
                raise DataError('invalid property', path + [key])

        if 'desired' in data:
            check_desired(data['desired'], ep, path + ['desired'])

        if 'children' in data:
            check_children(data['children'], ep, path + ['children'])

    def check_collection(data, ep, path):
        if not isinstance(data, Mapping):
            raise DataError('invalid collection', path)

        auth_collection(ep, path)

        for key, entity in data.iteritems():
            if not isinstance(key, basestring):
                raise DataError('invalid key type', path + [key])

            check_entity(entity, ep, path + [key])

    def auth_collection(ep, path):
        if alicorn:
            return

        if ep.access == 'protected':
            raise AccessError('administrator access required', path)

        if ep.access == 'shared':
            if write:
                raise AccessError('administrator access required', path)
            return

        if ep.access == 'tenant/owner':
            if role != 'owner':
                raise AccessError('tenant-owner role required', path)
            return

        if ep.access == 'tenant/user':
            if not tenant:
                raise AccessError('tenant access required', path)

            if write and role == 'operator':
                raise AccessError('operators cannot modify tenant data', path)

            return

        if ep.access == 'user/rw':
            if user is None:
                raise AccessError('user access required', path)
            return

        if ep.access == 'user/ro':
            if user is None:
                raise AccessError('user access required', path)

            if write:
                raise AccessError('administrator access required', path)

    def auth_entity(ep, path):
        if alicorn:
            return

        if ep.table.name == 'tenant':
            if not tenant:
                raise AccessError('missing tenant credentials', path)

            if tenant != path[-1]:
                raise AccessError('invalid tenant credentials', path)

            return

        if ep.table.name == 'user':
            if not user:
                raise AccessError('missing user credentials', path)

            if user != path[-1]:
                raise AccessError('invalid user credentials', path)

            return

    check_children(data, schema.root, [])


def validate_dbdict_fragment(creds, data, path, write):
    """
    Same as ``validate_dbdict()`` but automatically converts data with
    path to one large chunk of data.
    """

    for elem in reversed(path):
        data = {elem: data}

    return validate_dbdict(creds, data, write)


# vim:set sw=4 ts=4 et:
