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

from sqlalchemy.exc import DatabaseError
from werkzeug.exceptions import NotFound, Unauthorized
from simplejson import loads, dumps
from functools import wraps
from os.path import dirname
from time import time
from collections import Mapping

from sparkle.common import *
from sparkle.util import call_sync, remove_nulls
from sparkle.schema import schema
from sparkle.rest import Flaskful, json_response
from sparkle.auth import sign_token
from sparkle.validate import validate_json_patch, validate_dbdict_fragment
from sparkle.patch import Pointer, normalize_path
from sparkle.dbdict import preprocess_patch, Children

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
            # Get root of the database dictionary mapping.
            root = Children(manager.db, schema.root)

            # Convert placeholders in the input patch to actual uuids.
            # Returns mapped placeholders for client to orient himself.
            uuids = preprocess_patch(root, patch)

            # Apply the patch.
            Pointer(root).patch(patch)

            # Determine our transaction id.
            txid = int(manager.db.execute('SELECT cork();').fetchone()[0])

            # Register our completion watch.
            call_sync(manager.listener.register, txid)

            try:
                try:
                    # Attempt to commit the transaction.
                    manager.db.commit()
                except DatabaseError, e:
                    # We have nothing better for now.
                    raise DataError(e.orig.diag.message_primary, [])
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

            # Read patch from the client.
            patch = loads(flask.request.data)

            # And make sure it really is a patch and not something
            # totally weird that would blow up later.
            validate_json_patch(patch)

            # Alas, we need to poke into the patch a bit before we allow
            # it to execute.  Namely, we need to rebase paths and consult
            # access control rules.
            for op in patch:
                if 'path' in op:
                    # Determine whether this operation is a mutation.
                    # We have different access rules for reading and writing.
                    write = ('test' != op['op'])

                    # We default to empty dictionary when no value is given.
                    # This is however just for access control checks.
                    value = op.get('value', {})

                    # Adjust the path to include endpoint prefix.
                    op['path'] = prefix + normalize_path(op['path'])

                    # Validate the value at the path.
                    validate_dbdict_fragment(creds, value, op['path'], write)

                if 'from' in op:
                    # Adjust source path the same way as above.
                    op['from'] = prefix + normalize_path(op['from'])

                    # And verify that we can access the source data.
                    # XXX: This could be very broken as we should validate
                    #      access to all child entities and not just the root.
                    validate_dbdict_fragment(creds, {}, op['from'], False)

                # Remove None keys from values of non-merge operations.
                if 'value' in op and 'merge' != op['op']:
                    op['value'] = remove_nulls(op['value'])

            # Run the patch and hope for the best?
            return {'uuids': apply_patch(patch)}

        @app.require_credentials(manager)
        @convert_errors
        def collection_handler(credentials={}, **keys):
            jpath = endpoint.to_jpath(keys)[:-2]

            if 'GET' == flask.request.method:
                # Make sure all access control restrictions are applied.
                validate_dbdict_fragment(credentials, {}, jpath, False)

                try:
                    # Let the manager deal with model access and data
                    # retrieval.  XXX: Access control is broken there, BTW.
                    data = call_sync(manager.list_collection, path, keys)
                except KeyError:
                    raise PathError('not found', jpath)

                # We promised not to return any nulls in the output.
                return remove_nulls(data)

            if 'POST' == flask.request.method:
                # Read data from client.
                data = loads(flask.request.data)

                # Make sure it's at least a litle sane.
                if not isinstance(data, Mapping):
                    raise DataError('invalid entity', jpath)

                # Desired state must be present when POSTing.
                if 'desired' not in data:
                    raise DataError('desired state missing', jpath + ['desired'])

                # When primary keys must be specified by the user, make sure
                # that they are present and do not invent them on our own.
                if endpoint.table.user_pkey:
                    djpath = jpath + ['desired']
                    if isinstance(endpoint.table.pkey, basestring):
                        if endpoint.table.pkey not in data['desired']:
                            raise DataError('primary key missing', djpath + [endpoint.table.pkey])
                    else:
                        for key in endpoint.table.pkey:
                            if key not in data['desired']:
                                raise DataError('primary key missing', djpath + [key])

                if 'desired' in data:
                    # Extract the primary key placeholder.
                    pkey = data['desired'].get(endpoint.table.pkey, 'POST')
                else:
                    # Or default to one named 'POST'.
                    pkey = 'POST'

                # Get rid of keys with None values.
                data = remove_nulls(data)

                # Calculate hypothetical destination path.
                post_path = jpath + [pkey]

                # Make sure all access control restrictions are applied.
                validate_dbdict_fragment(credentials, data, post_path, True)

                # This is what the patch should look like:
                patch = [{'op': 'add', 'path': post_path, 'value': data}]

                # Apply the patch and return resulting set of UUIDs.
                return {'uuids': apply_patch(patch)}

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, jpath)

        @app.require_credentials(manager)
        @convert_errors
        def entity_handler(credentials={}, **keys):
            jpath = endpoint.to_jpath(keys)[:-1]

            if 'GET' == flask.request.method:
                # Make sure that we can access the entity.
                validate_dbdict_fragment(credentials, {}, jpath, False)

                try:
                    # Get data from manager who can access the model.
                    data = call_sync(manager.get_entity, path, keys)
                except KeyError:
                    raise PathError('not found', jpath)

                # Strip keys with None values.
                return remove_nulls(data)

            if 'DELETE' == flask.request.method:
                # Make sure that we can write to that entity.
                validate_dbdict_fragment(credentials, {}, jpath, True)

                # Construct patch that would remove it.
                patch = [{'op': 'remove', 'path': jpath}]

                # Execute as usual.
                return {'uuids': apply_patch(patch)}

            if 'PATCH' == flask.request.method:
                return common_patch(credentials, jpath)

        # Rename handlers to satisfy Flask.
        collection_handler.__name__ = 'c_' + '_'.join(path)
        entity_handler.__name__     = 'e_' + '_'.join(path)

        return collection_handler, entity_handler

    # Generate entity and collection endpoints.
    for path, endpoint in schema.endpoints.iteritems():
        # Convert list path to a string suitable for Flask routing.
        rule = path_to_rule(path)

        # Prepare entity and collection handlers from schema.
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

        # Bestow project owner role for alicorns
        if user.desired.get('alicorn'):
            role = 'owner'
        else:
            role = member.desired['role']

        return make_token_result({
            'tenant': tenant,
            'role': role
        })

    # Endpoint to dump the schema for clients to orient themselves.
    @app.route_json('/v1/schema')
    def dump_schema():
        return schema.root.public

    # Ta-dah?
    return app


# vim:set sw=4 ts=4 et:
