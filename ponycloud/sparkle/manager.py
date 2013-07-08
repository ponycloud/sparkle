#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import task, reactor
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.internet.defer import Deferred

from listener import ChangelogListener, ListenerError
from notifier import Notifier

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

    def __init__(self, router, db, notifier, authkeys):
        """
        Stores the event sinks for later use.
        """
        self.db = db
        self.router = router

        """
        Listener for applying changes in database
        """
        self.listener = ChangelogListener(db.engine.url)
        self.listener.add_callback(self.apply_changes)
        self.listener.listen()

        # This is where we keep the configuration data.
        self.model = Model()

        self.notifier = notifier

        #
        # In addition to the configuration, we keep some info about hosts.
        # Specifically, their routing ids, sequence numbers, incarnation and
        # most importantly, map of current states they provide plus a reverse
        # map of desired state they are interested in.
        #
        self.incarnation = uuidgen()
        self.hosts = {}
        self.host_to_row = {}
        self.row_to_host = {}

        # Install watches that manage row ownership for replication.
        self.add_watches()


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
            self.add_watches()

            self.notifier.load(self.model)
            self.notifier.start()

        # Configure where to go from there.
        d.addCallbacks(success, failure)
    # /def schedule_relate


    def twilight_update(self, uuid, incarnation, changes, seq, sender):
        """Handler for current state replication from Twilights."""

        # Periodically notify new hosts.
        if uuid not in self.hosts:
            print 'twilight %s appeared' % uuid
            loop = task.LoopingCall(self.send_changes, uuid, [])
            reactor.callLater(0, loop.start, 15.0)

            self.hosts[uuid] = {
                'incarnation': None,
                'current': {},
                'inseq': 0,
                'outseq': 1,
                'loop': loop,
            }

        # Update host record.
        host = self.hosts[uuid]
        host['route'] = sender

        if host['incarnation'] != incarnation or host['inseq'] != seq:
            for table, objects in host['current'].iteritems():
                for pkey in objects:
                    self.model[table].update_row(pkey, 'current', None)

            host['current'] = {}

            if seq > 0:
                print 'requesting resync with twilight %s' % uuid
                self.router.send({'event': 'resync'}, sender)
                host['incarnation'] = incarnation
                host['inseq'] = 0
                return

        # Update the model with changes from Twilight.
        for table, pkey, state, part in changes:
            if part is None:
                host['current'].setdefault(table, set()).discard(pkey)
            else:
                host['current'].setdefault(table, set()).add(pkey)

            self.model[table].update_row(pkey, state, part)

        # Bump the sequence and save current incarnation of the peer.
        host['incarnation'] = incarnation
        host['inseq'] += 1


    def validate_path(self, path, keys):
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


    def apply_changes(self, data):
        """
        Applies changes to the model and forwards them to Twilights.

        Sparkle is not supposed to send current state,
        so make sure you only update desired state through here.
        """

        # Apply non-delete changes to the model,
        # so that we know how new rows map to individual hosts.
        self.model.load([ch for ch in data if ch[3]])

        # Sort out which changes should go to which hosts.
        hosts = {}
        for change in data:
            for h in self.row_to_host.get(change[:2], []):
                hosts.setdefault(h, []).append(change)

        # Apply deletion changes to the model after assesing what hosts
        # to send notifications to.  The host-row mappings are removed here.
        self.model.load([ch for ch in data if not ch[3]])

        # Send the change bulks.
        for host, ch in hosts.items():
            self.send_changes(host, ch)


    def send_changes(self, host, changes):
        """Sends a bulk of changes to given host."""
        # Get the routing key for the host. It is different from it's uuid.
        if host not in self.hosts:
            return

        route = self.hosts[host]['route']

        # Send a nice, warm message with all the goodies.
        self.router.send({
            'event': 'update',
            'incarnation': self.incarnation,
            'seq': self.hosts[host]['outseq'],
            'changes': changes,
        }, route)
        self.hosts[host]['outseq'] += 1


    def twilight_resync(self, host, sender):
        """Sends complete desired state for given Twilight."""
        print 'sending complete desired state for %s' % host

        changes = []
        for name, pkey in self.host_to_row.get(host, []):
            table = self.model[name]
            changes.append((name, pkey, 'desired', table[pkey].desired))

        self.router.send({
            'incarnation': self.incarnation,
            'seq': 0,
            'event': 'update',
            'changes': changes,
        }, sender)

        if host in self.hosts:
            self.hosts[host]['outseq'] = 1


    def add_watches(self):
        """Install event handlers that manage row ownership."""

        def assign(table, row, host):
            self.row_to_host.setdefault((table.name, row.pkey), set()).add(host)
            self.host_to_row.setdefault(host, set()).add((table.name, row.pkey))

        def after_host_update(table, row):
            assign(table, row, row.pkey)

        def after_host_owned_row_update(table, row):
            assign(table, row, row.desired['host'])

        def after_nic_role_update(table, row):
            bond = self.model['bond'][row.desired['bond']]
            assign(table, row, bond.desired['host'])


        def watch(table, handler):
            @wraps(handler)
            def wrapper(table, row):
                host = self.row_to_host.pop((table.name, row.pkey), set([None])).pop()
                self.host_to_row.get(host, set()).discard((table.name, row.pkey))
                if row.desired is not None:
                    handler(table, row)

            table.on_after_row_update(wrapper)
            for row in table.itervalues():
                handler(table, row)

        watch(self.model['host'], after_host_update)
        watch(self.model['bond'], after_host_owned_row_update)
        watch(self.model['nic'], after_host_owned_row_update)
        watch(self.model['nic_role'], after_nic_role_update)


    @backtrace
    def list_collection(self, path, keys):
        """
        Called from API to obtain list of collection items.
        """

        # Get the leading path plus name of the collection, validate the
        # path for access control to work and fetch the collection.
        path, collection = path[:-1], path[-1]
        self.validate_path(path, keys)
        rows = self.model[collection].list(**{k: keys[k] for k in path[-1:]})
        return [row.to_dict() for row in rows]


    @backtrace
    def get_entity(self, path, keys):
        """
        Called from API to obtain entity description.
        """

        # Validate path leading to the entity for access control.
        self.validate_path(path, keys)

        try:
            name = path[-1]
            return self.model[name][keys[name]].to_dict()
        except KeyError:
            raise PathError('%s/%s not found' % (name, keys[name]))


    @backtrace
    @database_operation
    def create_or_update_entity(self, path, keys, value):
        """
        Called from API to modify an entity.
        """
        name = path[-1]

        # Validate entity path.
        # This is essential in order to enforce access control, because
        # the updated entity will not be allowed to reference any other
        # parent.
        self.validate_path(path, keys)

        def recurse(path, keys, value):
            # Make sure the value is valid and references correct parent.
            check_and_fix_parent(path, keys, value)

            # Get info about the entity.
            name = path[-1]
            table = self.model[name]
            entity = getattr(self.db, name)

            # This just does not make sense for virtual entities.
            if table.virtual:
                raise UserError('cannot change virtual entity')

            # Get the current object.
            obj = None
            if name in keys:
                row = table.get(keys[name])
                if row is None or row.desired is None:
                    if name == 'uuid':
                        raise UserError('%s/%s not found' \
                                            % (table.name, keys[name]))
                else:
                    obj = entity.filter_by(**{table.pkey: keys[name]}).one()

            if obj is None:
                # Create completely new row.
                obj = entity.insert(**value)
                keys[table.name] = getattr(obj, table.pkey)
            else:
                # Apply the update to individual columns.
                # Ignore uuid updates, there is no way we are going to allow
                # user to change them.  It could compromise security.
                for c in obj.c:
                    if c.name in value and c.name != 'uuid':
                        setattr(obj, c.name, value[c.name])

            # Validate that the children tables are defined in model
            for child_table in value.get('children', {}):
                if child_table not in table.children:
                    raise UserError('invalid child')
                # ...and that each child is a list
                if not isinstance(value.get('children')[child_table], list):
                    raise UserError('list expected')
                # Get current children so we can delete those not defined in "value"
                original_children = self.model[child_table].list(**{table.name: keys[name]})
                entity = getattr(self.db, child_table)
                # Each row in the children list has to be a dict
                for child in value.get('children')[child_table]:
                    if not isinstance(child, dict):
                        raise UserError('object expected')

                    child_pkey = self.model[child_table].pkey
                    child_keys = dict(keys.items() + {table.name: getattr(obj, table.pkey)}.items())
                    if child_pkey in child:
                        child_keys[child_table] = child[child_pkey]
                    # Dive into the next level of children
                    recurse(path + [child_table], child_keys, child)

                # Delete those children that were originally there and are not defined
                # in the input data
                for o_child in original_children:
                    found = False
                    for c_child in value.get('children')[child_table]:
                        if c_child.get(self.model[child_table].pkey) == o_child.desired.get(self.model[child_table].pkey):
                            found = True
                            break
                    if not found:
                        entity.filter_by(**{self.model[child_table].pkey: o_child.desired.get(self.model[child_table].pkey)})\
                        .delete(synchronize_session=False)

        recurse(path, keys, value)

        # Attempt to commit the transaction.
        # This is where consistency is checked on the database side.
        self.db.commit()

        # Make sure we update in-memory desired state.
        self.listener.poll()

        # Return new desired state of the entity.
        return self.model[name][keys[path[-1]]].desired
    # /def create_or_update_entity


    @backtrace
    @database_operation
    def delete_entity(self, path, keys):
        """
        Called by API to delete entities.
        """

        # Validate entity path.
        self.validate_path(path, keys)

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

        # Attempt to commit the transaction.
        # This is where consistency is checked on the database side.
        self.db.commit()

        # Make sure we update in-memory desired state.
        self.listener.poll()

        # Well...
        return None
    # /def delete_entity

# /class Manager

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
