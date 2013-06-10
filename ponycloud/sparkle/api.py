#!/usr/bin/python -tt

"""
Sparkle RESTful API

This module implements backend for Sparkle RESTful API.
"""

__all__ = ['make_sparkle_app']

from twisted.internet.threads import blockingCallFromThread
from twisted.internet import reactor

from werkzeug.exceptions import BadRequest, InternalServerError, NotFound
from ponycloud.common.rest import Flaskful
from flask import request

from ponycloud.sparkle.manager import ManagerError, UserError, PathError

from os.path import dirname
from functools import wraps

import cjson
import re


AUTOMAGIC_ENDPOINTS = [
    '/disk/<varchar:disk>',
    '/volume/<uuid:volume>',
    '/cpu-profile/<uuid:cpu_profile>',
    '/storage-pool/<uuid:storage_pool>',
    '/storage-pool/<uuid:storage_pool>/disk/<varchar:disk>',
    '/host/<uuid:host>',
    '/host/<uuid:host>/nic/<varchar:nic>',
    '/host/<uuid:host>/bond/<uuid:bond>',
    '/host/<uuid:host>/bond/<uuid:bond>/nic/<varchar:nic>',
    '/host/<uuid:host>/bond/<uuid:bond>/nic-role/<uuid:nic_role>',
    '/host/<uuid:host>/disk/<varchar:disk>',
    '/image/<uuid:image>',
    '/switch/<uuid:switch>',
    '/switch/<uuid:switch>/network/<uuid:network>',
    '/switch/<uuid:switch>/network/<uuid:network>/route/<uuid:route>',
    '/tenant/<uuid:tenant>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/vdisk/<uuid:vdisk>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/cluster-instance/<uuid:cluster_instance>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/vnic/<uuid:vnic>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/vnic/<uuid:vnic>/address/<uuid:address>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/vnic/<uuid:vnic>/switch/<uuid:switch>',
    '/tenant/<uuid:tenant>/image/<uuid:image>',
    '/tenant/<uuid:tenant>/quota/<uuid:quota>',
    '/tenant/<uuid:tenant>/volume/<uuid:volume>',
    '/tenant/<uuid:tenant>/volume/<uuid:volume>/vdisk/<uuid:vdisk>',
    '/tenant/<uuid:tenant>/volume/<uuid:volume>/extent/<uuid:extent>',
    '/tenant/<uuid:tenant>/cluster/<uuid:cluster>',
    '/tenant/<uuid:tenant>/cluster/<uuid:cluster>/cluster-instance/<uuid:cluster_instance>',
    '/tenant/<uuid:tenant>/switch/<uuid:switch>',
    '/tenant/<uuid:tenant>/switch/<uuid:switch>/network/<uuid:network>',
    '/tenant/<uuid:tenant>/switch/<uuid:switch>/network/<uuid:network>/route/<uuid:route>',
    '/tenant/<uuid:tenant>/member/<varchar:member>',
    '/user/<varchar:user>',
    '/user/<varchar:user>/member/<uuid:member>',
]


def get_endpoints():
    """
    Returns processed endpoints from above.
    """

    endpoints = []

    for rule in AUTOMAGIC_ENDPOINTS:
        ep = re.sub('([^/]+)/<', '\\1<', rule[1:])
        ep = ep.split('/')
        out = []
        for i in xrange(len(ep)):
            if '<' in ep[i]:
                out.append(re.split('[<>:]', ep[i])[2])

        endpoints.append((rule, out))

    return endpoints


def convert_exceptions(fn):
    """Decorator that changes manager errors to HTTP exceptions."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except PathError, e:
            raise NotFound(e.message)
        except UserError, e:
            raise BadRequest(e.message)
        except ManagerError, e:
            raise InternalServerError(e.message)

    return wrapper


def call(fn, *args, **kwargs):
    return blockingCallFromThread(reactor, fn, *args, **kwargs)


def make_collection_handler(manager, path):
    """
    Creates collection endpoint handler for given path.
    """

    @convert_exceptions
    def handler(**keys):
        # Set tenant to None by in order to properly display
        # entities below the tenant level.
        keys.setdefault('tenant', None)

        if 'GET' == request.method:
            return call(manager.list_collection, path, keys)
        elif 'POST' == request.method:
            data = cjson.decode(request.data)
            return call(manager.create_or_update_entity, path, keys, data)

    handler.__name__ = 'c_' + '_'.join(path)
    return handler


def make_entity_handler(manager, path):
    """
    Creates entity endpoint handler for given path.
    """

    @convert_exceptions
    def handler(**keys):
        # Set tenant to None by in order to properly display
        # entities below the tenant level.
        keys.setdefault('tenant', None)

        if 'GET' == request.method:
            return call(manager.get_entity, path, keys)
        elif 'PUT' == request.method:
            data = cjson.decode(request.data)
            return call(manager.create_or_update_entity, path, keys, data)
        elif 'DELETE' == request.method:
            return call(manager.delete_entity, path, keys)

    handler.__name__ = 'e_' + '_'.join(path)
    return handler


def make_sparkle_app(manager):
    """
    Constructs Sparkle RESTful API site.
    """

    # Create the application.
    app = Flaskful(__name__)
    app.debug = True

    # Just re-use the converters for now.
    app.url_map.converters['uuid'] = app.url_map.converters['string']
    app.url_map.converters['varchar'] = app.url_map.converters['string']

    # Generate endpoints.
    for rule, path in get_endpoints():
        app.route_json(rule, methods=['GET', 'PUT', 'DELETE'])(make_entity_handler(manager, path))
        app.route_json(dirname(rule) + '/', methods=['GET', 'POST'])(make_collection_handler(manager, path))

    # Custom top-level endpoint.
    @app.route_json('/')
    def index():
        return {
            'application': 'Sparkle',
            'capabilities': [],
        }

    # Simple reflection of the data endpoints.
    @app.route_json('/_endpoints')
    def endpoints():
        return AUTOMAGIC_ENDPOINTS


    # Debugging endpoint that dumps all data in the Sparkle model.
    @app.route_json('/_dump')
    def dump():
        return call(manager.model.dump)

    # Ta-dah?
    return app
# /def make_sparkle_app

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
