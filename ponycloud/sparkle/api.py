#!/usr/bin/python -tt

"""
Sparkle RESTful API

This module implements backend for Sparkle RESTful API.
"""

__all__ = ['make_sparkle_app']

from twisted.internet.threads import blockingCallFromThread
from twisted.internet import reactor

from contextlib import contextmanager
from os.path import dirname

from ponycloud.common.rest import Flaskful
from flask import request

import cjson
import re


AUTOMAGIC_ENDPOINTS = [
    '/platform/disk/<varchar:disk>',
    '/platform/volume/<uuid:volume>',
    '/platform/cpu-profile/<uuid:cpu_profile>',
    '/platform/storage-pool/<uuid:storage_pool>',
    '/platform/storage-pool/<uuid:storage_pool>/disk/<varchar:disk>',
    '/platform/host/<uuid:host>',
    '/platform/host/<uuid:host>/nic/<varchar:nic>',
    '/platform/host/<uuid:host>/raid/<uuid:raid>',
    '/platform/host/<uuid:host>/raid/<uuid:raid>/logical-volume/<uuid:logical_volume>',
    '/platform/host/<uuid:host>/raid/<uuid:raid>/disk/<varchar:disk>',
    '/platform/host/<uuid:host>/bond/<uuid:bond>',
    '/platform/host/<uuid:host>/bond/<uuid:bond>/nic/<varchar:nic>',
    '/platform/host/<uuid:host>/bond/<uuid:bond>/role/<uuid:nic_role>',
    '/public/image/<uuid:image>',
    '/public/switch/<uuid:switch>',
    '/public/switch/<uuid:switch>/network/<uuid:network>',
    '/public/switch/<uuid:switch>/network/<uuid:network>/route/<uuid:route>',
    '/tenant/<uuid:tenant>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/vdisk/<uuid:vdisk>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/cluster/<uuid:cluster_instance>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/vnic/<uuid:vnic>',
    '/tenant/<uuid:tenant>/instance/<uuid:instance>/vnic/<uuid:vnic>/address/<uuid:address>',
    '/tenant/<uuid:tenant>/image/<uuid:image>',
    '/tenant/<uuid:tenant>/quota/<uuid:quota>',
    '/tenant/<uuid:tenant>/volume/<uuid:volume>',
    '/tenant/<uuid:tenant>/volume/<uuid:volume>/vdisk/<uuid:vdisk>',
    '/tenant/<uuid:tenant>/volume/<uuid:volume>/extent/<uuid:extent>',
    '/tenant/<uuid:tenant>/cluster/<uuid:cluster>',
    '/tenant/<uuid:tenant>/cluster/<uuid:cluster>/instance/<uuid:cluster_instance>',
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
# /def get_endpoints

def make_collection_handler(manager, path):
    """
    Creates collection endpoint handler for given path.
    """

    def handler(**keys):
        if 'GET' == request.method:
            return blockingCallFromThread(reactor, manager.list_collection, path, keys)
        elif 'POST' == request.method:
            return blockingCallFromThread(reactor, manager.create_entity, path, keys, cjson.decode(request.data))

    handler.__name__ = 'c_' + '_'.join(path)
    return handler
# /def make_collection_handler

def make_entity_handler(manager, path):
    """
    Creates entity endpoint handler for given path.
    """

    def handler(**keys):
        if 'GET' == request.method:
            return blockingCallFromThread(reactor, manager.get_entity, path, keys)
        elif 'PUT' == request.method:
            return blockingCallFromThread(reactor, manager.update_entity, path, keys, cjson.decode(request.data))
        elif 'DELETE' == request.method:
            return blockingCallFromThread(reactor, manager.delete_entity, path, keys)

    handler.__name__ = 'e_' + '_'.join(path)
    return handler
# /def make_entity_handler

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

    # Ta-dah?
    return app
# /def make_sparkle_app

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
