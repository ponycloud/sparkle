#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import task, reactor
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.internet.defer import Deferred

from listener import ChangelogListener, ListenerError
#from notifier import Notifier

from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm.exc import NoResultFound

from ponycloud.common.util import uuidgen
from ponycloud.common.model import Model

from functools import wraps

import traceback
import re


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

        # Authkeys from configuration are stored here
        self.authkeys = authkeys

        """
        Listener for applying changes in database
        """
        self.listener = ChangelogListener(db.engine.url)
        self.listener.add_callback(self.apply_changes)
        self.listener.listen()

        # This is where we keep the configuration data.
        self.model = Model()

        # This is how we notify users via websockets
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

            for name, table in model.iteritems():
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

            #self.notifier.load(self.model)
            #self.notifier.start()

        # Configure where to go from there.
        d.addCallbacks(success, failure)


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
        for host, ch in hosts.iteritems():
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


    def list_collection(self, path, keys):
        """
        Called from API to obtain list of collection items.
        """

        path, collection = path[:-1], path[-1]
        rows = self.model[collection].list(**{k: keys[k] for k in path[-1:]})
        return {row.pkey: row.to_dict() for row in rows}


    def get_entity(self, path, keys):
        """
        Called from API to obtain entity description.
        """
        try:
            name = path[-1]
            return self.model[name][keys[name]].to_dict()
        except KeyError:
            raise KeyError('%s/%s not found' % (name, keys[name]))


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
