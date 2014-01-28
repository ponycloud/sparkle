#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['DatabaseListener']

from select import select
from simplejson import loads

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from twisted.internet import task, reactor
from twisted.internet.defer import Deferred
from twisted.internet.interfaces import IFileDescriptor, IReadDescriptor
from zope.interface import implements

from sparkle.util import call_sync


class DatabaseListener(object):
    """
    Listener for watching the changes in the database.
    """

    implements(IReadDescriptor, IFileDescriptor)

    def __init__(self, conn_string):
        """
        Create connection to db using connection string.
        """

        # Save the connection string.
        self.conn_string = str(conn_string)

        # Place for the future connection instance.
        self.conn = None

        # Start with empty set of change listeners.
        self.callbacks = set()

        # Start with empty mapping of txid to waiting transactions.
        self.transactions = {}

        # Transaction identificator of the last processed change.
        self.txid = None

        # Accumulated changes from the same transaction.
        self.changes = []

        # Looping call that take care of corking changes made by
        # administrators directly in the database.
        self.corker = None

    def add_callback(self, callback):
        """Register callback to notify of every transaction's changes."""
        self.callbacks.add(callback)

    def remove_callback(self, callback):
        """De-register previously added callback."""
        self.callbacks.discard(callback)

    def register(self, txid):
        """
        Register deferred for given transaction id.
        """

        if txid in self.transactions:
            raise KeyError('someone is already waiting for txid %i' % txid)

        self.transactions[txid] = Deferred()

    def abort(self, txid):
        """
        Stop waiting for given transaction id.
        """

        if txid not in self.transactions:
            raise KeyError('no-one is waiting for txid %i' % txid)

        self.transactions.pop(txid)

    def wait(self, txid):
        """Produce deferred that will wait until the transaction completes."""
        return self.transactions[txid]

    def connect(self):
        """
        Connect to database and start receiving changes.
        """

        print 'database listener connecting'

        self.conn = connect(self.conn_string)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with self.conn.cursor() as curs:
            curs.execute("LISTEN changelog;")

        print 'database listener connected successfully'

    def fileno(self):
        return self.conn.fileno()

    def shutdown(self):
        print 'shutting down database listener'
        reactor.removeReader(self)
        self.conn.close()
        self.conn = None
        self.corker.stop()
        self.corker = None

    def connectionLost(self, reason):
        pass

    def start(self):
        """
        Start forwarding changes to registered handlers.
        """

        # Connect to the database.
        self.connect()

        # Start receiving descriptor events.
        reactor.addReader(self)

        # Start periodic corking of changes made in the database
        # manually by administrators.
        self.corker = task.LoopingCall(self.cork)
        self.corker.start(5.0)

    def cork(self):
        """
        Insert read barrier to the queue of pending changes.
        Return transaction identificator.
        """

        with self.conn.cursor() as curs:
            curs.execute('SELECT cork() AS cork;')
            return int(curs.fetchone()[0])

    def flush(self):
        """
        Flush transaction changes to callbacks.
        """

        # Unblock any waiting threads.
        if self.txid in self.transactions:
            self.transactions[self.txid].callback(self.txid)

        # Notify listeners about new completed transaction.
        for callback in self.callbacks:
            reactor.callLater(0, callback, self.changes)

        # Reset accumulated changes and transaction id.
        self.changes = []
        self.txid = None

    def doRead(self):
        """
        Receive fresh changes from the database.
        """

        self.conn.poll()

        if len(self.conn.notifies) > 0:
            # There are some pending notifications, plan one more poll
            # right after we process this one just to make sure we have
            # consumed all the events.
            reactor.callLater(0, self.doRead)

        while len(self.conn.notifies) > 0:
            # Pop one notification off the queue.
            item = loads(self.conn.notifies.pop(0).payload)

            # Parse first two arguments - type of operation and transaction.
            op   = item[0]
            txid = int(item[1])

            if self.txid is None:
                # First transaction should not trigger a flush.
                self.txid = txid

            if self.txid != txid:
                # Flush on txid change.
                self.flush()
                self.txid = txid

            if op == 'u':
                # Accumulate changes.
                entity, action, pkey = item[2:5]
                payload = loads(item[-1])

                if action == 'DELETE':
                    change = (entity, payload[pkey], 'desired', None)
                else:
                    change = (entity, payload[pkey], 'desired', payload)

                self.changes.append(change)

            elif op == 'cork':
                # Flush on every cork operation.
                self.flush()

    def logPrefix(self):
        return 'listener'


# vim:set sw=4 ts=4 et:
