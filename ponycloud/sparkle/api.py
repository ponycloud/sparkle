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
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound, \
                                Unauthorized
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


def path_to_rule(path):
    """Convert list with path components to routing rule for Flask."""

    fullpath = ['']

    endpoint = schema.root
    covered  = set()
    for elem in path:
        endpoint = endpoint.children[elem]
        fullpath.append(elem)

        if isinstance(endpoint.table.pkey, basestring):
            fullpath.append('<string:%s>' % endpoint.table.name)
            covered.add(endpoint.table.name)
        else:
            keys = [key for key in endpoint.table.pkey if key not in covered]
            assert len(keys) == 1, \
                    "endpoint %r does not have it's keys covered" \
                        % ('/' + '/'.join(path[:(1 + path.index(elem))]))

            for key in keys:
                assert key in schema.tables, \
                        "primary key %s.%s is not named after another table" \
                            % (endpoint.table.name, key)

                fullpath.append('<string:%s>' % key)
                covered.add(key)

    return '/v1' + '/'.join(fullpath)

def remove_nulls(data):
    """Recursively remove None values from dictionary."""

    if not isinstance(data, Mapping):
        return data

    return {k: remove_nulls(v) for k, v in data.iteritems() if v is not None}

def make_json_schema(cache, credentials, write=True):
        """Prepare schema for specified conditions."""

        key = (tuple(sorted(credentials.iteritems())), write)
        if key in cache:
            return cache[key]

        return cache.setdefault(key, make_schema(credentials, write))


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
            apply_patch(Children(manager.db, schema.root), patch)
            manager.db.commit()
            return uuids
        except Exception, e:
            manager.db.rollback()
            raise

    def make_handlers(path):
        endpoint = schema.resolve_path(path)

        def common_patch(credentials, keys, cache, jpath):
            """PATCH handler for both collection and entity endpoints."""

            patch = loads(flask.request.data)
            validate_patch(patch)

            for op in patch:
                write = ('TEST' != op['op'])
                jschema = make_json_schema(cache, credentials, write)
                op['path'] = jpath + split(op['path'])
                validate_dbdict_fragment(jschema, op.get('value', {}), op['path'])

                if 'from' in op:
                    jschema = make_json_schema(cache, credentials, False)
                    op['from'] = jpath + split(op['from'])
                    validate_dbdict_fragment(jschema, {}, op['from'])

            return {'uuids': apply_valid_patch(patch)}

        @app.require_credentials(manager)
        def collection_handler(credentials={}, **keys):
            jpath = reduce(add, [[t, keys.get(t), 'children'] for t in path])[:-2]
            cache = {}

            if 'GET' == flask.request.method:
                jschema = make_json_schema(cache, credentials, False)
                validate_dbdict_fragment(jschema, {}, jpath)

                data = blockingCallFromThread(reactor, manager.list_collection, path, keys)
                return remove_nulls(data)

            if 'POST' == flask.request.method:
                data = loads(flask.request.data)
                if 'desired' in data:
                    pkey = data['desired'].get(endpoint.table.pkey, 'POST')
                else:
                    pkey = 'POST'

                post_path = jpath + [pkey]
                jschema = make_json_schema(cache, credentials, True)
                validate_dbdict_fragment(jschema, {}, post_path)
                patch = [{'op': 'add', 'path': post_path, 'value': data}]
                return {'uuids': apply_valid_patch(patch)}

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, keys, cache, jpath)

        @app.require_credentials(manager)
        def entity_handler(credentials={}, **keys):
            jpath = reduce(add, [[t, keys.get(t), 'children'] for t in path])[:-1]
            cache = {}

            if 'GET' == flask.request.method:
                jschema = make_json_schema(cache, credentials, False)
                validate_dbdict_fragment(jschema, {}, jpath)

                data = blockingCallFromThread(reactor, manager.get_entity, path, keys)
                return remove_nulls(data)

            if 'DELETE' == flask.request.method:
                jschema = make_json_schema(cache, credentials, True)
                validate_dbdict_fragment(jschema, {}, jpath)

                patch = [{'op': 'remove', 'path': jpath}]
                return {'uuids': apply_valid_patch(patch)}

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, keys, cache, jpath)

        collection_handler.__name__ = 'c_' + '_'.join(path)
        entity_handler.__name__     = 'e_' + '_'.join(path)

        return collection_handler, entity_handler

    # Generate entity and collection endpoints.
    for path, endpoint in schema.endpoints.iteritems():
        rule = path_to_rule(path)

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

    def make_token_result(credentials):
        """Create response with specified credentials."""

        payload = dumps(credentials)
        apikey = manager.authkeys['apikey']
        validity = 3600

        return {
            'token': sign_token(payload, apikey, validity),
            'valid': int(time() + validity),
        }

    # Issues token for detected credentials.
    # Cannot be used to generate tenant token, only to renew it.
    @app.route_json('/v1/token')
    @app.require_credentials(manager)
    def token(credentials={}):
        return make_token_result(credentials)

    # Issues tenant token if credentials match (user is tenant's member,
    # alirn or the supplied token already is a token for this tenant).
    @app.route_json('/v1/tenant/<string:tenant>/token')
    @app.require_credentials(manager)
    def tenant_token(credentials={}, tenant=None):
        tenant_row = blockingCallFromThread(reactor,
                                            manager.model['tenant'].get,
                                            tenant)

        if tenant_row is None:
            raise NotFound('invalid tenant')

        if 'tenant' in credentials:
            return make_token_result(credentials)

        user = blockingCallFromThread(reactor,
                                      manager.model['user'].get,
                                      credentials['user'])

        if user is None or not user.desired.get('alicorn'):
            member = blockingCallFromThread(reactor,
                                            manager.model['member'].one,
                                            tenant=tenant,
                                            user=credentials['user'])

            if member is None:
                raise Unauthorized('you are not a member of the tenant')

        return make_token_result({
            'tenant': tenant,
            'role': member.desired['role']
        })

    # Endpoint to dump the schema for clients to orient themselves.
    @app.route_json('/v1/schema')
    def dump_schema():
        return schema.root.public

    # Debugging endpoint that dumps all data in the Sparkle model.
    if app.debug:
        @app.route_json('/v1/dump')
        def dump():
            return blockingCallFromThread(reactor, manager.model.dump)

    # Ta-dah?
    return app


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
