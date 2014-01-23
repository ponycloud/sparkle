#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import task, reactor
from twisted.internet.threads import deferToThread, blockingCallFromThread

from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm.exc import NoResultFound

from ponycloud.common.util import uuidgen
from ponycloud.common.model import Model

from ponycloud.sparkle.listener import ChangelogListener, ListenerError
from ponycloud.sparkle.communicator import Communicator
from ponycloud.sparkle.twilight import Twilight
from ponycloud.sparkle.notifier import Notifier

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
                if not table.schema.virtual:
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

    def ensure_host(self, data, sender):
        uuid = data.get('uuid')
        incarnation = data.get('incarnation')

        if uuid not in self.hosts:
            print 'twilight %s appeared' % uuid
            communicator = Communicator(self.incarnation, incarnation, sender, self.router)
            self.hosts[uuid] = Twilight(uuid, self.model, communicator)
        elif self.hosts[uuid].communicator is None:
            communicator = Communicator(self.incarnation, incarnation, sender, self.router)
            self.hosts[uuid].communicator = communicator

    def process_event(self, data):
        self.hosts[data['uuid']].process_changes(data)


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
        self.hosts[host].send_changes(changes)

    def add_watches(self):
        """Install event handlers that manage row ownership."""

        def assign(table, row, hosts=None):
            """ Add row for host or hosts
                If hosts is none, this will assign given row to all known hosts """
            def _set_state(table, row, host):
                """ Add row to single specific host """
                self.row_to_host.setdefault((table.name, row.pkey), set()).add(host)
                self.hosts.setdefault(host, Twilight(host, self.model)).add_row(table.name, row.pkey)

            if hosts is None:
                for host in self.hosts:
                    _set_state(table, row, host)
            elif isinstance(hosts, list):
                for host in hosts:
                    _set_state(table, row, host)
            else:
                    _set_state(table, row, hosts)

        def watch(table):
            handler = table.get_watch_handler(self.model, assign)
            @wraps(handler)
            def wrapper(table, row):
                host = self.row_to_host.pop((table.name, row.pkey), set([None])).pop()
                if host:
                    self.hosts[host].delete_row(table.name, row.pkey)
                if row.desired is not None:
                    handler(table, row)
            table.on_after_row_update(wrapper)
            for row in table.itervalues():
                handler(table, row)

        # Watch these tables for changes
        watch_tables = ['host', 'bond', 'nic', 'nic_role', 'storage_pool', 'disk']
        for table_name in watch_tables:
            watch(self.model[table_name])

    def list_collection(self, path, keys):
        """
        Called from API to obtain list of collection items.
        """

        path, collection = path[:-1], path[-1]
        rows = self.model[collection].list(**keys)
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
