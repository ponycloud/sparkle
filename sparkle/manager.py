#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Manager']

from twisted.internet import task, reactor
from twisted.internet.threads import deferToThread
from sqlalchemy.exc import OperationalError

from sparkle.model import Model, Row
from sparkle.schema import schema
from sparkle.listener import DatabaseListener
from sparkle.twilight import Twilight
from sparkle.placement import Placement


class Manager(object):
    """
    The main application logic of Sparkle.
    """

    def __init__(self, router, db, notifier, apikey):
        """
        Stores the event sinks for later use.
        """
        self.db = db
        self.router = router

        # API secret key.
        self.apikey = apikey

        # This is where we keep the configuration and status data.
        self.model = Model()

        # Create listener for applying changes in database.
        self.listener = DatabaseListener(self.db.engine.url)
        self.listener.add_callback(self.apply_changes)

        # This is how we notify users via websockets
        self.notifier = notifier
        self.notifier.set_model(self.model)
        self.notifier.start()

        # Map of hosts by their uuids so that we can maintain some
        # state information about our communication with them.
        self.hosts = {}

        # Mapping of `(name, pkey)` pairs to hosts the rows were placed on.
        # Very relevant to the placement algorithm.
        self.rows = {}

        # Hook up the placement implementation with manager.
        self.placement = Placement(self)

        # Watch for model updates to be forwarded to placement and
        # individual hosts.
        self.model.add_callback(self.on_row_changed)


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
            # Drop any cached changes.
            self.db.rollback()

            # Attempt to connect database change listener so that we
            # don't miss out on any notifications.
            self.listener.connect()

            # Stash for the extracted data.
            data = []

            for name, table in schema.tables.iteritems():
                if not table.virtual:
                    for row in getattr(self.db, name).all():
                        part = {c.name: getattr(row, c.name) for c in row.c}
                        pkey = table.primary_key(part)
                        data.append((name, pkey, 'desired', part))

            # Return complete data set to be loaded into the model.
            return data

        # Attempt the load the data.
        d = deferToThread(load)

        # Load failure handler traps just the OperationalError from
        # database, other exceptions need to be propagated so that we
        # don't break debugging.
        def failure(fail):
            print 'database connection failed, retrying in 15 seconds'
            reactor.callLater(15, self.schedule_load)

        # In case of success
        def success(data):
            print 'data successfully loaded'
            self.model.load(data)
            self.model.commit()

            # Start processing database changes.
            self.listener.start()

        # Configure where to go from there.
        d.addCallbacks(success, failure)


    def apply_changes(self, changes):
        """Incorporate changes from database into the model."""
        self.model.load(changes)
        self.model.commit()


    def on_row_changed(self, old, new):
        """
        Trigger placement algorithm and forward the change to hosts that
        the row have been placed on.
        """

        # Update placement and possible send deletes out immediately.
        self.placement.on_row_changed(old, new)

        # When desired state changed, we need to send out updates to
        # hosts that have the row placed on them.
        if new.desired and new.desired != old.desired:
            # Prepare the change in the usual format.
            change = (new.table.name, new.pkey, 'desired', new.desired)

            # Find what hosts should receive the update.
            for host in self.rows.get((new.table.name, new.pkey), []):
                if host in self.hosts:
                    # Distribute the change to relevant hosts.
                    self.hosts[host].send_changes([change])


    def receive(self, message, sender):
        if message['uuid'] not in self.hosts:
            self.hosts[message['uuid']] = Twilight(self, message['uuid'])
        self.hosts[message['uuid']].receive(message, sender)


    def bestow(self, host, row, owner):
        """
        Place selected row on a host with defined owner row.

        It is possible to define both row and owner as a `(name, pkey)`
        tuple instead.  Host can be either it's uuid or a Twilight instance.
        """

        if isinstance(host, basestring):
            if host not in self.hosts:
                self.hosts[host] = Twilight(self, host)
            host = self.hosts[host]

        if isinstance(row, Row):
            row = (row.table.name, row.pkey)

        if isinstance(owner, Row):
            owner = (owner.table.name, owner.pkey)

        print 'bestow %r to %s for %r' % (row, host.uuid, owner)

        hosts = self.rows.setdefault(row, set())
        hosts.add(host.uuid)

        owners = host.desired_state.setdefault(row, set())

        if len(owners) == 0:
            name, pkey = row
            if pkey in self.model[name]:
                desired = self.model[name][pkey].desired
                if desired:
                    host.send_changes([(name, pkey, 'desired', desired)])

        owners.add(owner)

    def withdraw(self, host, row, owner):
        """
        Remove row placement on given host for specified owner.

        As with `bestow()` the host, row and owner can be supplied as
        either identificators or corresponding objects.
        """

        if isinstance(host, basestring):
            host = self.hosts[host]

        if isinstance(row, Row):
            row = (row.table.name, row.pkey)

        if isinstance(owner, Row):
            owner = (owner.table.name, owner.pkey)

        print 'withdraw %r from %s for %r' % (row, host.uuid, owner)

        owners = host.desired_state.setdefault(row, set())
        owners.discard(owner)

        if 0 == len(owners):
            del host.desired_state[row]
            host.send_changes([(row[0], row[1], 'desired', None)])

        hosts = self.rows.setdefault(row, set())
        hosts.discard(host.uuid)

        if 0 == len(hosts):
            del self.rows[row]


    def list_collection(self, path, keys):
        """
        Called from API to obtain list of collection items.
        """

        # Verify that collection parent exists.
        self.model.path_row(path[:-1], keys)

        # Find endpoint for the collection itself.
        endpoint = schema.resolve_path(path)

        if endpoint.parent.table is None:
            # Top-level collections do not have any parents.
            rows = self.model[endpoint.table.name].list()
        else:
            # Filter using the endpoint filter and parent relationship.
            pname = endpoint.parent.table.name
            filter = dict(endpoint.filter)
            filter.update({pname: keys[pname]})
            rows = self.model[endpoint.table.name].list(**filter)

        return {row.pkey: row.to_dict() for row in rows}


    def get_entity(self, path, keys):
        """Called from API to obtain entity description."""
        return self.model.path_row(path, keys).to_dict()


# vim:set sw=4 ts=4 et:
