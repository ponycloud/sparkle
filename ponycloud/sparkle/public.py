#!/usr/bin/python -tt

__all__ = ['make_public_api']

from ponycloud.common.rest import Flaskful

from werkzeug.routing import BaseConverter, ValidationError
from werkzeug.exceptions import NotImplemented, NotFound, Forbidden

from twisted.internet import reactor
from twisted.internet.threads import blockingCallFromThread

from uuid import UUID

import os.path
import flask
import re

class UuidConverter(BaseConverter):
    '''Validates `uuid` URL argument type.'''

    def to_python(self, value):
        try:
            return unicode(UUID(value))
        except ValueError:
            raise ValidationError()

    def to_url(self, value):
        return unicode(UUID(value))

class MacaddrConverter(BaseConverter):
    '''Validates the `macaddr` (for MAC address) URL argument type.'''

    def to_python(self, value):
        if not re.match('^[0-9a-f]{2}(:[0-9a-f]{2}){5,}$', value):
            raise ValidationError()
        return value

    def to_url(self, value):
        return unicode(value)


def make_rule(path):
    """Creates URL rule for entity path."""

    # Start with an empty path.
    rule = []

    # Iterate over each path component and fill in the rule with
    # correct primary keys selected.
    for i in xrange(len(path)):
        # Add name of the table.
        rule.append(path[i][0].replace('_', '-'))

        # Partial paths for expanded *:1 mappings.
        if path[i][1] is None:
            break

        # Find primary keys not given above.
        for col in path[i][1].c:
            if not col.primary_key:
                continue

            fkt = set([fk.column.table for fk in col.foreign_keys])
            if not fkt.intersection([t[1]._table for t in path[:i]]):
                rule.append('<%s:%s>' % (str(col.type).lower(),
                                         path[i][0].replace('-', '_')))

    # Return combined path.
    return '/' + '/'.join(rule)


def make_keypath(path, kwargs):
    """
    Creates configuration path with keys.

    This is essentially the parser for what make_rule produces.
    """

    # Start with empty key path.
    kp = []

    # Traverse all elements.
    for i in xrange(len(path)):
        for col in path[i][1].c:
            if not col.primary_key:
                continue

            fkt = set([fk.column.table for fk in col.foreign_keys])
            if not fkt.intersection([t[1]._table for t in path[:i]]):
                kp.append((path[i][0].replace('-', '_'),
                           kwargs[path[i][0].replace('-', '_')]))

    # Return complete key path.
    return kp


def find_config(config, path, kwargs):
    """Locates desired and current configuration of specified entity."""

    # Follow individual path elements to the desired part of configuration.
    for name, key in make_keypath(path, kwargs):
        # Check that the child exists.
        if name not in config['children']:
            raise NotFound

        # Advance to the child.
        config = config['children'][name].get(key)

        # If the key did not exist, fail.
        if config is None:
            raise NotFound

    # Return the target part of configuration.
    return config


def register_collection_handler(app, manager, path):
    """Registers collection handler endpoint."""

    # The handler that handles entity collection GETs and POSTs.
    def handler(**kwargs):
        # Handle listing the entities.
        if flask.request.method == 'GET':
            #
            # TODO: Under no circumstances touch manager directly from here.
            #       This piece of code is running in a completely different
            #       thread and things *will* break if we continue doing it.
            #
            #       Use blockingCallFromThread() and ask manager nicely!
            #

            try:
                # Find the parent entity.
                parent = find_config(manager.config, path[:-1], kwargs)

                # Look for the collection.
                if path[-1][0] not in parent['children']:
                    raise NotFound
            except NotFound:
                # Show none instead of hard-failing.
                return {'total': 0, 'items': {}}

            # The collection in question.
            collection = parent['children'][path[-1][0]]

            # Return the collection items.
            # TODO: All at once for now, paging support will come later.
            return {
                    'total': len(collection),
                    'items': dict([(k, {'desired': v['desired'],
                                        'current': v['current']}) \
                                   for k, v in collection.items()]),
            }

        if flask.request.method == 'POST':
            # TODO: Implement inserting completely new items.
            raise NotImplemented

        return {
            'type': 'collection',
            'kwargs': kwargs,
            'entity': path[-1][1]._table.name,
        }

    # Generate an unique handler name.
    handler.__name__ = '_'.join([p[0] for p in path]) + '_collection'

    # Bind the handler to one level above the entity with a trailing slash.
    # This translates paths such as /instance/<uuid> to /instance/ only.
    rule = os.path.dirname(make_rule(path)) + '/'
    app.route_json(rule, methods=['GET', 'POST'])(handler)


def register_instance_handler(app, manager, db, path):
    """Registers instance handler endpoint."""

    # The handler that takes care about entity GET, PUT, and DELETE.
    def handler(**kwargs):
        return {
            'type': 'instance',
            'kwargs': kwargs,
            'entity': path[-1][1]._table.name,
        }

    # Generate an unique handler name.
    handler.__name__ = '_'.join([p[0] for p in path]) + '_instance'

    # Register the handler with app on the /entity/<uuid> URL.
    app.route_json(make_rule(path), methods=['GET', 'PUT', 'DELETE'])(handler)


def register_db_entity(app, manager, db, path):
    """Registers single DB entity type with the API."""

    # Register handlers related to this entity type.
    register_collection_handler(app, manager, path)
    register_instance_handler(app, manager, db, path)

    # Recurse into _list elements.
    for name, col in path[-1][1].__dict__.items():
        if name[-5:] == '_list':
            name = name[:-5].replace('_', '-')
            subpath = path + [(name, getattr(db, col.property.target.name))]
            register_db_entity(app, manager, db, subpath)


def register_db_entities(app, manager, db, entities):
    """Registers selected DB entities to be accessible using RESTful API."""

    # Go through the top-level.
    for entity_name in entities:
        # Recursively add it to the API.
        path = [(entity_name, getattr(db, entity_name))]
        register_db_entity(app, manager, db, path)


def make_public_api(manager, db, top_endpoints):
    """Creates public RESTful API for given manager."""

    # Create the application object.
    app = Flaskful(__name__)

    # Add our URL converters for uuid, mac and other types.
    app.url_map.converters['uuid'] = UuidConverter
    app.url_map.converters['macaddr'] = MacaddrConverter
    app.url_map.converters['varchar'] = app.url_map.converters['string']
    app.url_map.converters['integer'] = app.url_map.converters['int']

    # TODO: Load access control list for database entities,
    #       perhaps directly from database comments or something?

    # Register the top-level endpoints recursively with the application.
    register_db_entities(app, manager, db, top_endpoints)

    # Custom top endpoint.
    @app.route_json('/')
    def index():
        return {
            'application': 'Sparkle',
            'capabilities': [],
        }

    # Return it with all methods registered.
    return app

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
