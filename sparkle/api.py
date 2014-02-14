#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__doc__ = """
Sparkle API

Most of our endpoints are generated from the schema and backed by a
custom JSON Patch implementation.  Schema-derived GETs receive their
data from Manager and return both desired and current state.

Other endpoints provide access to schema and means for token-based
authentication.
"""

__all__ = ['make_sparkle_app']

from twisted.internet import reactor
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound, \
                                Unauthorized
from jsonschema import ValidationError
from simplejson import loads, dumps
from functools import wraps
from os.path import dirname
from operator import add
from time import time
from collections import Mapping

from sparkle.common import *
from sparkle.util import call_sync
from sparkle.schema import schema
from sparkle.rest import Flaskful, json_response
from sparkle.auth import sign_token
from sparkle.validate import validate_json_patch, validate_dbdict_fragment
from sparkle.patch import Pointer, normalize_path
from sparkle.dbdict import preprocess_dbdict_patch, Children

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


def convert_errors(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ConflictError, e:
            return json_response(e.json, e.status)
        except PathError, e:
            return json_response(e.json, e.status)
        except UserError, e:
            return json_response(e.json, e.status)

    return wrapper


def make_sparkle_app(manager):
    """Construct Sparkle RESTful API site."""

    app = Flaskful(__name__)
    app.debug = True

    def apply_patch(patch):
        """
        Preprocess and apply validated JSON Patch to database.
        Returns dictionary with placeholder to uuid mappings.
        """

        try:
            # Convert placeholders in the input patch to actual uuids.
            # Returns mapped placeholders for client to orient himself.
            uuids = preprocess_dbdict_patch(patch)

            # Get root of the database dictionary mapping.
            root = Pointer(Children(manager.db, schema.root))

            # Apply the patch.
            root.patch(patch)

            # Determine our transaction id.
            txid = int(manager.db.execute('SELECT cork();').fetchone()[0])

            # Register our completion watch.
            call_sync(manager.listener.register, txid)

            try:
                # Attempt to commit the transaction.
                manager.db.commit()
            except:
                # Transaction failed, remove the completion watch.
                call_sync(manager.listener.abort, txid)

                # Re-raise the exception.
                raise

            # Wait for the transaction to propagate.
            call_sync(manager.listener.wait, txid)

            # Stop waiting for the transaction.
            call_sync(manager.listener.abort, txid)

            # Return mapped uuids to the client now, when all data safely
            # hit the model and he will be able to retrieve them.
            return uuids

        except:
            # Roll back the transaction on any error.
            manager.db.rollback()

            # Re-raise the exception.
            raise

    def make_handlers(path):
        endpoint = schema.resolve_path(path)

        def common_patch(creds, prefix):
            """
            PATCH handler for both collection and entity endpoints.
            """

            patch = loads(flask.request.data)
            validate_json_patch(patch)

            for op in patch:
                write = ('test' != op['op'])
                value = op.get('value', {})
                op['path'] = prefix + normalize_path(op['path'])
                validate_dbdict_fragment(creds, value, op['path'], write)

                if 'from' in op:
                    op['from'] = prefix + normalize_path(op['from'])
                    validate_dbdict_fragment(creds, {}, op['from'], False)

            return {'uuids': apply_patch(patch)}

        @app.require_credentials(manager)
        @convert_errors
        def collection_handler(credentials={}, **keys):
            jpath = endpoint.to_jpath(keys)[:-2]

            if 'GET' == flask.request.method:
                validate_dbdict_fragment(credentials, {}, jpath, False)

                try:
                    data = call_sync(manager.list_collection, path, keys)
                except KeyError:
                    raise PathError('not found', jpath)

                return remove_nulls(data)

            if 'POST' == flask.request.method:
                data = loads(flask.request.data)

                if 'desired' in data:
                    pkey = data['desired'].get(endpoint.table.pkey, 'POST')
                else:
                    pkey = 'POST'

                post_path = jpath + [pkey]
                validate_dbdict_fragment(credentials, data, post_path, True)
                patch = [{'op': 'add', 'path': post_path, 'value': data}]
                return {'uuids': apply_patch(patch)}

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, keys, jpath)

        @app.require_credentials(manager)
        @convert_errors
        def entity_handler(credentials={}, **keys):
            jpath = endpoint.to_jpath(keys)[:-1]

            if 'GET' == flask.request.method:
                validate_dbdict_fragment(credentials, {}, jpath, False)

                try:
                    data = call_sync(manager.get_entity, path, keys)
                except KeyError:
                    raise PathError('not found', jpath)

                return remove_nulls(data)

            if 'DELETE' == flask.request.method:
                validate_dbdict_fragment(credentials, {}, jpath, True)
                patch = [{'op': 'remove', 'path': jpath}]
                return {'uuids': apply_patch(patch)}

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, keys, jpath)

        collection_handler.__name__ = 'c_' + '_'.join(path)
        entity_handler.__name__     = 'e_' + '_'.join(path)

        return collection_handler, entity_handler

    # Generate entity and collection endpoints.
    for path, endpoint in schema.endpoints.iteritems():
        rule = path_to_rule(path)

        collection_handler, entity_handler = make_handlers(path)

        if endpoint.table.virtual:
            methods = ['GET']
        else:
            methods = ['GET', 'DELETE', 'PATCH']

        app.route_json(rule, methods=methods)(entity_handler)

        if endpoint.table.virtual:
            methods = ['GET']
        else:
            methods = ['GET', 'POST', 'PATCH']

        app.route_json(dirname(rule) + '/', methods=methods)(collection_handler)

    # Top-level endpoint for capabilitites reporting.
    @app.route_json('/')
    def index():
        return {
            'application': 'Sparkle',
            'capabilities': ['v1'],
        }

    # Endpoint for the imaginary root entity.
    @app.route_json('/v1/')
    def root():
        return {}

    def make_token_result(credentials):
        """Create response with specified credentials."""

        payload = dumps(credentials)
        apikey = manager.apikey
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
        tenant_row = call_sync(manager.model['tenant'].get, tenant)

        if tenant_row is None:
            raise NotFound('invalid tenant')

        if 'tenant' in credentials:
            return make_token_result(credentials)

        user = call_sync(manager.model['user'].get, credentials['user'])

        if user is None or not user.desired.get('alicorn'):
            member = call_sync(manager.model['member'].one,
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
            result = {}
            for tname, pkey, part, state in call_sync(manager.model.dump):
                if not schema.tables[tname].endpoints:
                    continue

                table = result.setdefault(tname, {})
                row = table.setdefault(pkey, {})
                row[part] = state

            return result

    # Ta-dah?
    return app


# vim:set sw=4 ts=4 et:
