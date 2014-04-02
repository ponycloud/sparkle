#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Twilight']

from twisted.internet import task
from uuid import uuid4

from sparkle.schema import schema


class Twilight(object):
    """Device for communication with an individual host worker."""

    def __init__(self, manager, uuid):
        # Store arguments for later use.
        self.manager = manager
        self.uuid = uuid

        # Start with some completely bogus peer uuid that will be replaced
        # as soon as we receive our first message.
        self.peer = str(uuid4())

        # Start with bogus IDs that will cause an early resync.
        self.local_incarnation = str(uuid4())
        self.remote_incarnation = str(uuid4())

        # Does not really matter due to unique incarnations above,
        # but be consistent with Twilight's communicator just in case.
        self.local_sequence = 1
        self.remote_sequence = 0

        # Current state rows we have inserted into the model because
        # client have sent them to us.
        self.current = set()

        # Desired state rows we have been assigned from the placement
        # algorithm via the manager.  Dictionary of tables with sets
        # of assigned row primary keys.
        self.desired = {}

        # New changes to the desired state that have not yet been
        # send to the peer.  Used to send changes per-transaction.
        self.pending = set()

        # Send keep-alive message every few seconds.
        self.keep_alive = task.LoopingCall(self.send_changes, [])
        self.keep_alive.start(15.0)

    def on_row_changed(self, name, pkey):
        """Called to notify us about a changed row."""
        self.pending.add((name, pkey))

    def send_pending_changes(self):
        """
        Send any pending changes in one transaction.
        """

        changes = []

        for name, pkey in self.pending:
            if pkey in self.desired.get(name, set()):
                desired = self.manager.model[name][pkey].desired
                changes.append((name, pkey, 'desired', desired))
            else:
                changes.append((name, pkey, 'desired', None))

        self.pending.clear()

        if changes:
            self.send_changes(changes)

    def send(self, message):
        """Send a message to our peer."""
        self.manager.router.send(message, self.peer)

    def send_changes(self, changes=[]):
        """
        Send a bulk of changes to the peer and bump the sequence number.
        """

        self.send({
            'event': 'update',
            'incarnation': self.local_incarnation,
            'seq': self.local_sequence,
            'changes': changes,
        })

        self.local_sequence += 1

    def receive(self, message, peer):
        """
        Called from outside to deliver message from the peer.
        """

        # Update route to the peer.
        self.peer = peer

        # Extract type of the event.
        event = message['event']

        # Determine what kind of message this is.
        if event == 'update':
            return self.update(message)

        if event == 'resync':
            return self.resync()

        print '[host %r] unknown event type %r' % (self.uuid, event)

    def resync(self):
        """
        Perform full desired state resync.

        In other words, reset outgoing sequence number to 0 to indicate an
        initial full state message and then send all of desired state at once.
        """

        self.local_sequence = 0

        changes = []
        for name, pkeys in self.desired.iteritems():
            for pkey in pkeys:
                if pkey in self.manager.model[name]:
                    part = self.manager.model[name][pkey].desired
                    changes.append((name, pkey, 'desired', part))

        self.send_changes(changes)

    def update(self, message):
        """
        Apply changes from the message.

        Remote incarnation and sequence number is validated and instead of
        applying changes we might end up requesting resync and don't touch
        the model at all.
        """

        if message['seq'] == 0:
            self.remote_incarnation = message['incarnation']
            self.remote_sequence = 1
            changes = list(self.iter_valid_changes(message['changes']))
            self.replace_current(changes)
            return

        if message['incarnation'] != self.remote_incarnation:
            self.remote_incarnation = message['incarnation']
            self.remote_sequence = 0
            self.send({'event': 'resync'})
            return

        if message['seq'] != self.remote_sequence:
            self.remote_sequence = 0
            self.send({'event': 'resync'})
            return

        changes = list(self.iter_valid_changes(message['changes']))
        self.merge_current(changes)
        self.remote_sequence += 1

    def valid_row(self, name, pkey, state, part):
        """
        Determine validity of current state row for given table.

        We do not assume malicious Twilights because they reside on an
        isolated network and an attacker could do much more interesting
        things there.  This is more for finding bugs than anything else.
        """

        # Accept only desired state.
        if state != 'current':
            return False

        # We only accept data for a few tables.
        if name not in set(('host', 'nic', 'bond', 'nic_role', 'host_disk',
                            'host_storage_pool')):
            return False

        if part is not None:
            # Valid data must be a dict.
            if not isinstance(part, dict):
                return False

            if isinstance(schema.tables[name].pkey, basestring):
                # Primary key is a single column, which needs to
                # be present in the payload.
                if schema.tables[name].pkey not in part:
                    return False

                # And match the primary key specified in the change record.
                if part[schema.tables[name].pkey] != pkey:
                    return False

            else:
                # In this case, we have a multi-column primary key.
                for i in xrange(len(schema.tables[name].pkey)):
                    key = schema.tables[name].pkey[i]

                    # All key parts must be present in the payload.
                    if key not in part:
                        return False

                    # And match the parts given in the change record.
                    if part[key] != pkey[i]:
                        return False

        # Entity seems acceptable.
        return True

    def iter_valid_changes(self, changes):
        """
        Filter rows approved by ``self.valid_row()`` while converting
        composite primary keys from lists to tuples.
        """

        for change in changes:
            if len(change) == 4 and self.valid_row(*change):
                if isinstance(change[1], list):
                    change[1] = tuple(change[1])
                yield change
            else:
                print '[host %r] invalid update: %r' \
                            % (self.uuid, change)

    def merge_current(self, changes):
        """
        Load specified changes to the model and track what rows we own.
        """

        # Record the ownership information.
        for name, pkey, state, part in changes:
            if part is None:
                self.current.discard((name, pkey))
            else:
                self.current.add((name, pkey))

        # Load the changes into the model in one go.
        self.manager.overlay.load(changes)
        self.manager.overlay.commit()

    def replace_current(self, changes):
        """
        Merge specified changes to the model and remove our rows not found
        in this update.  Attempts to minimize changes to the model in order
        to prevent platform disruption due to large placement changes.

        Row ownership is updated the same way ``merge_current`` does.
        """

        # Determine what rows will be deleted during the update due to
        # them not being mentioned in the replacement changeset.
        update_rows = set([tuple(change[:2]) for change in changes])
        delete_rows = self.current.difference(update_rows)
        delete_changes = [(name, pkey, 'current', None)
                          for name, pkey in delete_rows]

        # Record the ownership information.
        self.current = update_rows
        for name, pkey, state, part in changes:
            if part is None:
                self.current.discard((name, pkey))

        # Load the changes into the model in one go.
        self.manager.overlay.load(changes + delete_changes)
        self.manager.overlay.commit()


# vim:set sw=4 ts=4 et:
