#!/usr/bin/python -tt

"""
Sparkle RESTful API

This module implements backend for Sparkle RESTful API.
Most of the information for the API is taken from sqlsoup reflection,
but some things, such as authentication and authorization need special
handling.
"""

__all__ = ['make_sparkle_api', 'make_sparkle_site']

from twisted.internet.threads import blockingCallFromThread
from twisted.internet import reactor

from contextlib import contextmanager
from os.path import dirname

from ponycloud.common.rest import Flaskful
from flask import request

import cjson

class NoAuth(object):
    """No authentication handler."""

    def authorize(self, request):
        """Always allow access."""
        return True

# /class NoAuth


# TODO: Implement proper authentication.
class TenantAuth(NoAuth): pass
class UserAuth(NoAuth): pass


class Node(object):
    """
    Generic API element.
    """

    def __init__(self, auth=None):
        self.auth = auth
        self.children = {}

    @contextmanager
    def path(self, name, auth=None):
        """
        Adds a single path element.
        """

        if auth is None:
            # Use default auth if none specified.
            auth = self.auth

        # Add the child.
        self.children[name] = PathNode(auth=auth)

        # Let user operate on the new child.
        yield self.children[name]


    @contextmanager
    def bind(self, name, table=None, nm=None, key=None, filters={}, \
                         keytype='uuid', auth=None):
        """
        Adds an entity binding element.

        Name of the table defaults to the endpoint name with dashes
        replaced with underscores.  If overriden, mounts a different table.

        The nm parameter set to a table name enables M:N mapping over that
        table.  Posting to M:N collections causes inserts into the mapping
        table, so make sure they contain just two keys or supply defaults.

        The key parameter allows you to set the name of primary key to
        use as the entity identificator in the API.  All other primary keys
        are derived from the entities higher.

        The filters parameter allows you to place restriction on the
        entity in question.

        The keytype property allows to change type of entity primary key.
        This really *should* match the database.

        If you screw up, you won't be told until you attempt to apply the
        API to the application.
        """

        if auth is None:
            # Use default auth if none specified.
            auth = self.auth

        if table is None:
            # Guess table name.
            table = name.replace('-', '_')

        # Add the child.
        self.children[name] = EntityNode(table, nm, key, filters, keytype, \
                                         auth=auth)

        # Let user operate on the new child.
        yield self.children[name]

# /class Node


class API(Node):
    """
    Top-level API entity

    Using proper with statements on the bind() and path() a RESTful API
    backed by a database and a manager can be quickly constructed and
    applied to a Flaskful application.
    """

    def relate(self, db):
        """
        Configures relations between database entities.

        Because sqlsoup needs to inspect foreign keys in order to
        establish relations, this function can fail with OperationError.

        You might want to retry in that case.
        """

        def recurse(level, parent):
            # Iterate over the child elements.
            for name, child in level.children.items():
                if isinstance(child, EntityNode):
                    # Child entity.
                    entity = getattr(db, child.table)

                    # Relate this child to the parent entity.
                    if parent is not None:
                        if child.nm is None:
                            getattr(db, parent.table).relate('RL_' + child.table, getattr(db, child.table))
                        else:
                            getattr(db, parent.table).relate('RL_' + child.table, getattr(db, child.table), secondary=getattr(db, child.nm)._table)

                if isinstance(child, EntityNode):
                    # Current level is an entity node, recurse with this
                    # parent entity.
                    recurse(child, child)

                else:
                    # Recurse with current parent, this is not an entity.
                    recurse(child, parent)
        # /def recurse

        recurse(self, None)
    # /def relate


    def install(self, manager, db, app):
        """
        Installs the API to given Flaskful application.
        """

        def path_to_rule(path):
            out = []

            for name, node in path:
                # Append name of the node.
                out.append(name)

                if isinstance(node, EntityNode):
                    # The node is an entity, append it's primary key info.
                    out.append('<%s:%s>' % (node.keytype, node.table))

            return '/' + '/'.join(out)
        # /def path_to_rule

        def collection_handler(path):
            def handler(**kwargs):
                if request.method == 'GET':
                    return blockingCallFromThread(reactor, manager.list_collection, path, kwargs)
                elif request.method == 'POST':
                    data = cjson.decode(request.data)
                    return blockingCallFromThread(reactor, manager.create_entity, path, kwargs, data)

            # Generate an unique name.
            handler.__name__ = '_'.join([p[0] for p in path]) + '_collection'

            # Return the handler to be registered.
            return handler
        # /def collection_handler

        def entity_handler(path):
            def handler(**kwargs):
                if request.method == 'GET':
                    return blockingCallFromThread(reactor, manager.get_entity, path, kwargs)
                elif request.method == 'PUT':
                    data = cjson.decode(request.data)
                    return blockingCallFromThread(reactor, manager.update_entity, path, kwargs, data)
                elif request.method == 'DELETE':
                    return blockingCallFromThread(reactor, manager.delete_entity, path, kwargs)

            # Generate an unique name.
            handler.__name__ = '_'.join([p[0] for p in path]) + '_entity'

            # Return the handler to be registered.
            return handler
        # /def entity_handler

        def recurse(level, path, parent):
            # Iterate over the child elements.
            for name, child in level.children.items():
                if isinstance(child, EntityNode):
                    # Path to the child.
                    subpath = path + [(name, child)]

                    # Entities cause us to emit both collection and entity
                    # handlers.  Reuse the generated rule.
                    rule = path_to_rule(subpath)
                    app.route_json(dirname(rule) + '/', methods=['GET', 'POST'])(collection_handler(subpath))
                    app.route_json(rule, methods=['GET', 'PUT', 'DELETE'])(entity_handler(subpath))

                else:
                    # Path to the non-entity child.
                    subpath = path + [(name, child)]

                if isinstance(child, EntityNode):
                    # Current level is an entity node, recurse with this
                    # parent entity.
                    recurse(child, subpath, child)
                else:
                    # Recurse with current parent, this is not an entity.
                    recurse(child, subpath, parent)
        # /def recurse

        recurse(self, [], None)
    # /def install

# /class API


class PathNode(Node):
    """
    API element that carry no special meaning.

    Handling of this element type is all in the API class.
    """
# /class PathNode


class EntityNode(Node):
    """
    API element that represents a nested entity.

    Handling of this element type is all in the API class.
    It carries several interesting attributes related to the DB model.
    """

    def __init__(self, table, nm, key, filters, keytype, auth=None):
        """
        Stores custom attributes.
        """

        # Initialize the generic node.
        Node.__init__(self, auth=auth)

        # Store custom arguments.
        self.keytype = keytype
        self.filters = filters
        self.table = table
        self.key = key
        self.nm = nm

# /class EntityNode


def make_sparkle_api():
    """
    Creates Sparkle public API description.

    Needs sqlsoup database and a compliant manager, which Sparkle's
    obviously is.
    """

    api = API(auth=UserAuth)

    with api.bind('tenant', auth=TenantAuth) as tenant:
        with tenant.bind('quota'): pass
        with tenant.bind('image'): pass
        with tenant.bind('user', table='tenant_user', key='user', keytype='varchar'): pass

        with tenant.bind('cluster') as cluster:
            with cluster.bind('instance', table='cluster_instance', key='instance'): pass

        with tenant.bind('volume') as volume:
            with volume.bind('extent'): pass
            with volume.bind('vdisk'): pass

        with tenant.bind('switch', nm='tenant_switch') as switch:
            with switch.bind('vnic'): pass

            with switch.bind('network') as network:
                with network.bind('route'): pass

        with tenant.bind('instance') as instance:
            with instance.bind('cluster', table='cluster_instance', key='cluster'): pass
            with instance.bind('vdisk'): pass

            with instance.bind('vnic') as vnic:
                with vnic.bind('address'): pass

    with api.path('public') as public:
        with public.bind('image', filters={'tenant': None}): pass

        with public.bind('switch', filters={'tenant': None}) as switch:
            with switch.bind('network') as network:
                with network.bind('route'): pass

    with api.bind('user', keytype='varchar') as user:
        pass

    with api.path('platform') as platform:
        with platform.bind('disk', filters={'raid': None}, keytype='varchar'): pass
        with platform.bind('volume'): pass
        with platform.bind('cpu-profile'): pass

        with platform.bind('host') as host:
            with host.bind('nic', keytype='varchar'): pass

            with host.bind('raid') as raid:
                with raid.bind('disk', keytype='varchar'): pass
                with raid.bind('logical-volume'): pass

            with host.bind('nic-failover') as nic_failover:
                with nic_failover.bind('logical-nic'): pass
                with nic_failover.bind('nic-aggregation'): pass

        with platform.bind('storage-pool') as storage_pool:
            with storage_pool.bind('disk', keytype='varchar'): pass

        return api
# /def make_sparkle_api


def make_sparkle_site(manager, db, api):
    """
    Constructs Sparkle RESTful API site from the API.
    """

    # Create the application.
    app = Flaskful(__name__)

    # Just re-use the converters for now.
    app.url_map.converters['uuid'] = app.url_map.converters['string']
    app.url_map.converters['varchar'] = app.url_map.converters['string']

    # Install endpoints.
    api.install(manager, db, app)

    # Custom top-level endpoint.
    @app.route_json('/')
    def index():
        return {
            'application': 'Sparkle',
            'capabilities': [],
        }

    # Ta-dah?
    return app
# /def make_sparkle_site

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
