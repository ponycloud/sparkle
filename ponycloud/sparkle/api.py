#!/usr/bin/python -tt

__doc__ = """
Sparkle API

Most of our endpoints are generated from the schema and backed by a
custom JSON Patch implementation.  Schema-derived GETs receive their
data from Manager and return both desired and current state.

Other endpoints provide access to schema and means for token-based
authentication.
"""

__all__ = ['make_sparkle_app']

from twisted.internet.threads import blockingCallFromThread
from twisted.internet import reactor
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound
from simplejson import loads, dumps
from functools import wraps
from os.path import dirname
from operator import add
from time import time
from collections import Mapping

from ponycloud.common.schema import schema

from ponycloud.sparkle.rest import Flaskful
from ponycloud.sparkle.auth import sign_token
from ponycloud.sparkle.patch import validate_patch, apply_patch, split
from ponycloud.sparkle.dbdict import validate_dbdict_fragment, make_schema, \
                                     preprocess_dbdict_patch, Children

import flask


def path_to_endpoint(path):
    """Convert list with path components to endpoint string for Flask."""
    return '/v1/' + '/'.join(reduce(add, [[x, '<string:%s>' % x] for x in path]))

def remove_nulls(data):
    """Recursively remove None values from dictionary."""

    if not isinstance(data, Mapping):
        return data

    return {k: remove_nulls(v) for k, v in data.iteritems() if v is not None}

def make_json_schema(cache, credentials, manager, write=True):
        """Prepare schema for specified conditions."""

        tenant = credentials.get('tenant')
        user = credentials.get('user')

        key = (tenant, user, write)
        if key in cache:
            return cache[key]

        alicorn = False
        if user is not None and user in manager.model['user']:
            alicorn = manager.model['user'][user].get_desired('alicorn', False)

        return cache.setdefault(key, make_schema(tenant, alicorn, write))


def make_sparkle_app(manager):
    """Construct Sparkle RESTful API site."""

    app = Flaskful(__name__)
    app.debug = True

    def apply_valid_patch(patch):
        """
        Preprocess and apply validated JSON Patch to database.
        Returns dictionary with placeholder to uuid mappings.
        """

        try:
            uuids = preprocess_dbdict_patch(patch)
            apply_patch(Children(manager.db), patch)
            manager.db.commit()
            return uuids
        except Exception, e:
            manager.db.rollback()
            raise

    def make_handlers(path):
        def common_patch(credentials, keys, cache, jpath):
            """PATCH handler for both collection and entity endpoints."""

            patch = loads(flask.request.data)
            validate_patch(patch)

            for op in patch:
                writep = ('TEST' != op['op'])
                jschema = make_json_schema(cache, credentials, manager, writep)
                op['path'] = jpath + split(op['path'])
                validate_dbdict_fragment(jschema, op.get('value', {}), op['path'])

                if 'from' in op:
                    jschema = make_json_schema(cache, credentials, manager, False)
                    op['from'] = jpath + split(op['from'])
                    validate_dbdict_fragment(jschema, {}, op['from'])

            return {'uuids': apply_valid_patch(patch)}

        @app.require_credentials(manager)
        def collection_handler(credentials={}, **keys):
            jpath = reduce(add, [[t, keys.get(t), 'children'] for t in path])[:-2]
            cache = {}

            if 'GET' == flask.request.method:
                jschema = make_json_schema(cache, credentials, manager, False)
                validate_dbdict_fragment(jschema, {}, jpath)

                data = blockingCallFromThread(reactor, manager.list_collection, path, keys)
                return remove_nulls(data)

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, keys, cache, jpath)

        @app.require_credentials(manager)
        def entity_handler(credentials={}, **keys):
            jpath = reduce(add, [[t, keys.get(t), 'children'] for t in path])[:-1]
            cache = {}

            if 'GET' == flask.request.method:
                jschema = make_json_schema(cache, credentials, manager, False)
                validate_dbdict_fragment(jschema, {}, jpath)

                data = blockingCallFromThread(reactor, manager.get_entity, path, keys)
                return remove_nulls(data)

            if 'DELETE' == flask.request.method:
                jschema = make_json_schema(cache, credentials, manager, True)
                validate_dbdict_fragment(jschema, {}, jpath)

                patch = [{'op': 'remove', 'path': jpath}]
                return {'uuids': apply_valid_patch(patch)}

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, keys, cache, jpath)

        collection_handler.__name__ = 'c_' + '_'.join(path)
        entity_handler.__name__     = 'e_' + '_'.join(path)

        return collection_handler, entity_handler

    # Generate entity and collection endpoints.
    for path in schema.iter_paths():
        rule = path_to_endpoint(path)

        collection_handler, entity_handler = make_handlers(path)

        methods = ['GET', 'DELETE', 'PATCH']
        app.route_json(rule, methods=methods)(entity_handler)

        methods = ['GET', 'POST', 'PATCH']
        app.route_json(dirname(rule) + '/', methods=methods)(collection_handler)

    # Custom top-level endpoint.
    @app.route_json('/')
    def index():
        return {
            'application': 'Sparkle',
            'capabilities': ['v1'],
        }

    # Issues token for detected credentials.
    # Cannot be used to generate tenant token, only to renew it.
    @app.route_json('/v1/token')
    @app.require_credentials(manager)
    def token(credentials={}):
        payload = dumps(credentials)
        apikey = manager.authkeys['apikey']
        validity = 3600

        return {
            'token': sign_token(payload, apikey, validity),
            'valid': int(time() + validity),
        }

    # Endpoint to dump the schema for clients to orient themselves.
    @app.route_json('/v1/schema')
    def endpoints():
        return schema


    # Debugging endpoint that dumps all data in the Sparkle model.
    if app.debug:
        @app.route_json('/v1/dump')
        def dump():
            return blockingCallFromThread(manager.model.dump)

    # Ta-dah?
    return app


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
