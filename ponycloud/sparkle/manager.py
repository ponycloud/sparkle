#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import task, reactor
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.internet.defer import Deferred

from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm.exc import NoResultFound

from ponycloud.common.util import uuidgen
from ponycloud.common.model import Model

from functools import wraps

import traceback
import re


class ManagerError(Exception):
    """Generic manager error."""

class UserError(ManagerError):
    """Manager failure caused by invalid input from user."""

class PathError(UserError):
    """User requested a non-existing entity or collection."""


def database_operation(fn):
    """
    Decorator for DB-related methods of the Manager class.

    Used help with transactions and to convert database exceptions to
    manager errors that are better suited for display to the end users.
    """

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            # Call the actual function.
            return fn(self, *args, **kwargs)

        except Exception, e:
            # Roll back the transaction.
            self.db.rollback()

            # Simplest of errors, record not found.
            if isinstance(e, NoResultFound):
                raise PathError('not found')

            # If the database is down, notify user gracefully.
            if isinstance(e, OperationalError):
                raise ManagerError('database is down')

            # If it's something else, blame the user.
            if isinstance(e, DatabaseError):
                message = e.orig.pgerror

                if 'DETAIL:' in message:
                    # If we have a detail, keep just that.
                    message = re.sub('.*DETAIL: *', '', message, re.S)

                # Strip the "ERROR:" or something at the start.
                message = re.sub('^[A-Z]+: *', '', message)

                # If we have multiple lines, keep just the first.
                # Rest is going to describe SQL statement, which user
                # don't need to know anything about.
                message = re.sub('\n.*', '', message, re.S)

                raise UserError(message)

            # Otherwise just re-raise the exception and hope for the best.
            raise

    return wrapper
# /def database_operation


def backtrace(fn):
    """
    Backtracing wrapper

    Since Twisted will eat some of our backtraces, we need to dump them
    ourselves.  This decorator will print backtrace for any non-manager
    error that is raised by the wrapped function.

    The original exception is always re-raised.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ManagerError:
            raise
        except:
            print '------[ manager exception ]----------------------------'
            traceback.print_exc()
            print '-------------------------------------------------------'
            raise

    return wrapper
# /def backtrace


def check_and_fix_parent(path, keys, value):
    """
    Makes sure that value has correct parent.

    If user supplied a wrong parent, exception is raised.
    If no parent have been specified, it is added.
    """

    # Verify that we've received an object and not something else.
    if not isinstance(value, dict):
        raise UserError('object expected')

    # Enforce the correct parent.
    if len(path) > 1:
        if value.setdefault(path[-2], keys[path[-2]]) != keys[path[-2]]:
            raise UserError('invalid %s, expected %s' \
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
        self.hosts = {}


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

            for name, table in model.items():
                if not table.virtual:
                    for row in getattr(self.db, name).all():
                        part = {c.name: getattr(row, c.name) for c in row.c}
                        pkey = table.primary_key(part)
                        table.update_row(pkey, 'desired', part)

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
            self.model.load(old_model.dump(['current']))
            self.incarnation = uuidgen()

        # Configure where to go from there.
        d.addCallbacks(success, failure)
    # /def schedule_relate


    def twilight_state_update(self, uuid, incarnation, changes, seq, sender):
        """Handler for current state replication from Twilights."""

        # Update host record.
        host = self.hosts.setdefault(uuid, {
            'incarnation': None,
            'objects': {},
            'seq': 0
        })
        host['route'] = sender

        if host['incarnation'] != incarnation or host['seq'] != seq:
            for table, objects in host['objects'].iteritems():
                for pkey in objects:
                    self.model[table].update_row(pkey, 'current', None)

            host['objects'] = {}

            if seq > 0:
                print 'requesting resync with twilight %s' % uuid
                self.router.send({'event': 'sparkle-resync'}, sender)
                host['incarnation'] = incarnation
                host['seq'] = 0
                return

        # Update the model with changes from Twilight.
        for table, pkey, state, part in changes:
            if part is None:
                host['objects'].setdefault(table, set()).discard(pkey)
            else:
                host['objects'].setdefault(table, set()).add(pkey)

            self.model[table].update_row(pkey, state, part)

        # Bump the sequence.
        host['seq'] += 1


    def _validate_path(self, path, keys):
        """
        Validates entity path.

        Raises PathError if specified path does not exist.
        That is, this validates that specified instance is actually under
        the specified tenant and so on.
        """

        for i in xrange(len(path)):
            lst = self.model[path[i]].list(**{k: keys[k] for k in path[i - 1:i]})
            if 0 == len(lst):
                raise PathError('%s/%s not found' % (path[i], keys[path[i]]))


    def _get_changes(self):
        """
        Returns changes in current transaction and flushes the changelog.
        """

        changes = []

        # Retrieve all changes done by the current transaction.
        for row in self.db.changelog.order_by(self.db.changelog.id).all():
            # We need to know the corresponding table in our model.
            table = self.model[row.entity]

            # Identify the primary key of the row.
            if row.new_data is None:
                pkey = table.primary_key(row.old_data)
            else:
                pkey = table.primary_key(row.new_data)

            # Create loadable entry.
            changes.append((row.entity, pkey, 'desired', row.new_data))

        # Changelog needs to be emptied afterwards.
        # There is a trigger that won't otherwise allow commit.
        self.db.changelog.delete()

        return changes


    def apply_changes(self, changes):
        """
        Applies changes to the model and forwards them to Twilights.

        Sparkle is not supposed to send current state,
        so make sure you only update desired state through here.
        """
        self.model.load(changes)

        # TODO: Send to Twilights.


    @backtrace
    def list_collection(self, path, keys, page=0):
        """
        Called from API to obtain list of collection items.
        """

        # Get the leading path plus name of the collection, validate the
        # path for access control to work and fetch the collection.
        path, collection = path[:-1], path[-1]
        self._validate_path(path, keys)
        rows = self.model[collection].list(**{k: keys[k] for k in path[-1:]})
        state = [row.to_dict() for row in rows]

        # Return limited results, 100 per page.
        limited = state[page * 100 : (page + 1) * 100]
        return {'total': len(state), 'items': limited}


    @backtrace
    def get_entity(self, path, keys):
        """
        Called from API to obtain entity description.
        """

        # Validate path leading to the entity for access control.
        self._validate_path(path, keys)

        try:
            name = path[-1]
            return self.model[name][keys[name]].to_dict()
        except KeyError:
            raise PathError('%s/%s not found' % (name, keys[name]))


    @backtrace
    @database_operation
    def update_entity(self, path, keys, value):
        """
        Called from API to modify an entity.
        """

        # Validate entity path.
        # This is essential in order to enforce access control, because
        # the updated entity will not be allowed to reference any other
        # parent.
        self._validate_path(path, keys)

        # Make sure the value is valid and references correct parent.
        check_and_fix_parent(path, keys, value)

        # Get info about the entity.
        name = path[-1]
        table = self.model[name]
        entity = getattr(self.db, name)

        # This just does not make sense for virtual entities.
        if table.virtual:
            raise UserError('cannot update virtual entity')

        # Get the current object.
        obj = entity.filter_by(**{table.pkey: keys[name]}).one()

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
        self.apply_changes(changes)

        # Return new desired state of the entity.
        return self.model[name][keys[path[-1]]].desired
    # /def update_entity


    @backtrace
    @database_operation
    def create_entity(self, path, keys, value):
        """
        Called by API to create new entities.
        """
        # TODO: Add recursion support using DB relates.

        # Validate entity path.
        self._validate_path(path[:-1], keys)

        # Make sure the value is valid and references correct parent.
        check_and_fix_parent(path, keys, value)

        # Get info about the entity.
        name = path[-1]
        table = self.model[name]
        entity = getattr(self.db, name)

        # Do not allow to create virtual entities.
        if table.virtual:
            raise UserError('cannot create virtual entity')

        # Make sure we do not set uuid, database will generate one for us.
        # The only exception is the host table. Hosts have their own uuids.
        if 'uuid' in value and table.name != 'host':
            del value['uuid']

        # Attempt creation of new entity.
        entity.insert(**value)

        # Get data from the changelog.
        changes = self._get_changes()

        # Attempt to commit the transaction.
        # This is where consistency is checked on the database side.
        self.db.commit()

        # Apply changes to the in-memory model.
        self.apply_changes(changes)

        # Return desired state of the new entity.
        for table, pkey, state, part in changes:
            if table == name:
                return part

        # Or just a poor None if we've failed (which is not very probable).
        return None
    # /def create_entity


    @backtrace
    @database_operation
    def delete_entity(self, path, keys):
        """
        Called by API to delete entities.
        """

        # Validate entity path.
        self._validate_path(path, keys)

        # Get info about the entity.
        name = path[-1]
        table = self.model[name]
        entity = getattr(self.db, name)

        # This just does not make sense for virtual entities.
        if table.virtual:
            raise UserError('cannot delete virtual entity')

        # Attempt deletion of the entity.
        entity.filter_by(**{table.pkey: keys[name]})\
                .delete(synchronize_session=False)

        # Get data from the changelog.
        changes = self._get_changes()

        # Attempt to commit the transaction.
        # This is where consistency is checked on the database side.
        self.db.commit()

        # Apply changes to the in-memory model.
        self.apply_changes(changes)

        # Well...
        return None
    # /def delete_entity

# /class Manager

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
