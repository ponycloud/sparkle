#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import task, reactor
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.internet.defer import Deferred

from sqlalchemy.exc import OperationalError, DatabaseError

from werkzeug.exceptions import NotFound, Forbidden, BadRequest, \
                                InternalServerError

from ponycloud.common.util import uuidgen
from ponycloud.common.model import Model

from functools import wraps

import re


def database_operation(fn):
    """
    Decorator for DB-related methods of the Manager class.
    """

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            # Call the actual function.
            return fn(self, *args, **kwargs)

        except Exception, e:
            # Roll back the transaction.
            self.db.rollback()

            # If the database is down, notify user gracefully.
            if isinstance(e, OperationalError):
                raise InternalServerError('database is down')

            # If it's something else, just forward it to the user.
            if isinstance(e, DatabaseError):
                if 'DETAIL:' in e.orig.pgerror:
                    raise BadRequest(re.sub('(.|\n)*DETAIL: *', '', e.orig.pgerror))
                raise BadRequest(re.sub('^[A-Z]+: *', '', e.orig.pgerror))

            # Otherwise just re-raise the exception and hope for the best.
            raise

    return wrapper
# /def database_operation


def check_and_fix_parent(path, keys, value):
    """
    Makes sure that value has correct parent.

    If user supplied a wrong parent, exception is raised.
    If no parent have been specified, it is added.
    """

    # Verify that we've received an object and not something else.
    if not isinstance(value, dict):
        raise BadRequest('object expected')

    # Enforce the correct parent.
    if len(path) > 1:
        if value.setdefault(path[-2], keys[path[-2]]) != keys[path[-2]]:
            raise BadRequest('invalid %s, expected %s' \
                                % (path[-2], keys[path[-2]]))


class Manager(object):
    """
    The main application logic of Sparkle.
    """

    def __init__(self, router, db):
        """
        Stores the event sinks for later use.
        """
        self.db = db
        self.router = router
        self.incarnation = uuidgen()
        self.model = Model()


    def start(self):
        """
        Launches startup-triggered asynchronous operations.
        """

        print 'starting manager'

        # We need to load data from database on startup.
        self.schedule_load()


    def schedule_load(self):
        """
        Schedules an asynchronous attempt to load DB data.

        If the load fails, it is automatically retried every 15
        seconds until it succeeds.  Call only once.
        """

        print 'scheduling data load'

        def load():
            # Create the replacement model.
            model = Model()

            for table, entities in model.table_map.items():
                # Ignore virtual tables, they do not exist in database.
                if table in model.virtual:
                    continue

                # Load the data for the `real' ones.
                for row in getattr(self.db, table).all():
                    row = {c.name: getattr(row, c.name) for c in row.c}
                    for ent in entities:
                        ent.notify(table, {}, {'desired': row})

            # Return finished model to replace the current one.
            return model

        # Attempt the load the data.
        d = deferToThread(load)

        # Load failure handler traps just the OperationalError from
        # database, other exceptions need to be propagated so that we
        # don't break debugging.
        def failure(fail):
            fail.trap(OperationalError)
            print 'data load failed, retrying in 15 seconds'
            reactor.callLater(15, self.schedule_load)

        # In case of success
        def success(new_model):
            print 'data successfully loaded'

            old_model = self.model
            self.model = new_model
            self.model.apply_changes(old_model.dump('current'))

        # Configure where to go from there.
        d.addCallbacks(success, failure)
    # /def schedule_relate


    def twilight_presence(self, msg, sender):
        """
        Called for every periodic Twilight presence announcement.

        Remembers routing ID for that given Twilight instance and
        starts fencing timer.
        """

        # Check if we already know about the Twilight.
        if msg['uuid'] not in self.hosts:
            print 'twilight %s appeared' % msg['uuid']

        # Make sure that the host exists and update route.
        self.hosts.setdefault(msg['uuid'], {})
        self.hosts[msg['uuid']]['route'] = sender


    def validate_path(self, path, keys):
        """
        Validates entity path.

        Raises NotFound if specified path does not exist.
        That is, this validates that specified instance is actually under
        the specified tenant and so on.
        """

        for child in path[1:]:
            if 0 == len(getattr(self.model, child).list(**keys)):
                raise NotFound('%s/%s not found' % (child, keys[child]))


    def _get_changes(self):
        """
        Returns changes in current transaction and flushes the changelog.
        """

        changes = []

        # Retrieve all changes done by the current transaction.
        # Mentioning only desired state (which is what this is about)
        # will cause Model to update only that part of entities.
        for row in self.db.changelog.order_by(self.db.changelog.id).all():
            old_data = {'desired': row.old_data}
            new_data = {'desired': row.new_data}
            changes.append((row.entity, old_data, new_data))

        # Changelog needs to be emptied afterwards.
        # There is a trigger that won't otherwise allow commit.
        self.db.changelog.delete()

        return changes


    def list_collection(self, path, keys, page=0):
        """
        Called from API to obtain list of collection items.
        """

        # Get the leading path plus name of the collection, validate the
        # path for access control to work and fetch the collection.
        path, collection = path[:-1], path[-1]
        self.validate_path(path, keys)
        state = getattr(self.model, collection).list(**keys)

        # Return limited results, 100 per page.
        limited = state[page * 100 : (page + 1) * 100]
        return {'total': len(state), 'items': limited}


    def get_entity(self, path, keys):
        """
        Called from API to obtain entity description.
        """

        # Validate path leading to the entity for access control.
        self.validate_path(path, keys)

        try:
            name = path[-1]
            return getattr(self.model, name).get(keys[name])
        except KeyError:
            raise NotFound('%s/%s not found' % (name, keys[name]))


    @database_operation
    def update_entity(self, path, keys, value):
        """
        Called from API to modify an entity.
        """

        # Validate entity path.
        # This is essential in order to enforce access control, because
        # the updated entity will not be allowed to reference any other
        # parent.
        self.validate_path(path, keys)

        # Make sure the value is valid and references correct parent.
        check_and_fix_parent(path, keys, value)

        # Get info about the entity.
        name = path[-1]
        node = getattr(self.model, name)
        entity = getattr(self.db, name)

        # This just does not make sense for virtual entities.
        if name in self.model.virtual:
            raise BadRequest('cannot update virtual entity')

        # Get the current object.
        obj = entity.filter_by(**{node.pkey[0]: keys[name]}).one()

        # Apply the update to individual columns.
        # Ignore uuid updates, there is no way we are going to allow
        # user to change them.  It could compromise security.
        for c in obj.c:
            if c.name in value and c.name != 'uuid':
                setattr(obj, c.name, value[c.name])

        # Get data from the changelog.
        changes = self._get_changes()

        # Attempt to commit the transaction.
        # This is where consistency is checked on the database side.
        self.db.commit()

        # Apply changes to the in-memory model.
        self.model.apply_changes(changes)

        # Return new desired state of the entity.
        return getattr(self.model, name).get(keys[path[-1]])['desired']
    # /def update_entity


    @database_operation
    def create_entity(self, path, keys, value):
        """
        Called by API to create new entities.
        """
        # TODO: Add recursion support using DB relates.

        # Validate entity path.
        self.validate_path(path[:-1], keys)

        # Make sure the value is valid and references correct parent.
        check_and_fix_parent(path, keys, value)

        # Get info about the entity.
        name = path[-1]
        node = getattr(self.model, name)
        entity = getattr(self.db, name)

        # Do not allow to create virtual entities.
        if name in self.model.virtual:
            raise BadRequest('cannot create virtual entity')

        # Make sure we do not set uuid, database will generate one for us.
        if 'uuid' in value:
            del value['uuid']

        # Attempt creation of new entity.
        entity.insert(**value)

        # Get data from the changelog.
        changes = self._get_changes()

        # Attempt to commit the transaction.
        # This is where consistency is checked on the database side.
        self.db.commit()

        # Apply changes to the in-memory model.
        self.model.apply_changes(changes)

        # Return desired state of the new entity.
        for table, old, new in changes:
            if table == name:
                return new['desired']

        # Or just a poor None if we've failed (which is not very probable).
        return None
    # /def create_entity


    @database_operation
    def delete_entity(self, path, keys):
        """
        Called by API to delete entities.
        """

        # Validate entity path.
        self.validate_path(path, keys)

        # Get info about the entity.
        name = path[-1]
        node = getattr(self.model, name)
        entity = getattr(self.db, name)

        # This just does not make sense for virtual entities.
        if name in self.model.virtual:
            raise BadRequest('cannot delete virtual entity')

        # Attempt deletion of the entity.
        entity.filter_by(**{node.pkey[0]: keys[name]})\
                .delete(synchronize_session=False)

        # Get data from the changelog.
        changes = self._get_changes()

        # Attempt to commit the transaction.
        # This is where consistency is checked on the database side.
        self.db.commit()

        # Apply changes to the in-memory model.
        self.model.apply_changes(changes)

        # Well...
        return None
    # /def delete_entity

# /class Manager

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
