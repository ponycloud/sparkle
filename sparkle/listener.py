#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

import select, psycopg2, psycopg2.extensions
from twisted.internet import reactor, task
from simplejson import loads

__all__ = ['ChangelogListener']


class ListenerError(Exception):
    """Generic listener error"""

class ChangelogListener:
    """
    Listener for watching the changes in the database
    and passing them further
    """
    def __init__(self, conn_string):
        """
        Create connection to db using connection string
        """

        # Connect to the database.
        self.conn = psycopg2.connect(str(conn_string))

        # Start with empty set of change listeners.
        self.callbacks = set()

        # Start with empty mapping of txid to waiting transactions.
        self.transactions = {}

        # Transaction identificator of the last processed change.
        self.txid = None

        # Accumulated changes from the same transaction.
        self.changes = []

        # Looping calls that take care of getting changes from the database.
        self.poller = None
        self.corker = None

    def add_callback(self, callback):
        """Register callback to notify of every transaction's changes."""
        self.callbacks.add(callback)

    def remove_callback(self, callback):
        """De-register previously added callback."""
        self.callbacks.discard(callback)

    def block_until_transaction(self, txid):
        """
        Produce deferred that will wait until the transaction completes.
        """

        if txid not in self.transactions:
            self.transactions[txid] = Deferred()

        return self.transactions[txid]

    def start(self):
        """
        Start forwarding changes to registered handlers.
        """

        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with self.conn.cursor() as curs:
            curs.execute("LISTEN changelog;")

        self.poller = task.LoopingCall(self.poll)
        self.poller.start(0.2)

        self.corker = task.LoopingCall(self.cork)
        self.corker.start(5.0)

    def stop(self):
        """
        Stop listening to transaction changes.
        """

        if self.poller is not None:
            self.poller.stop()
            self.poller = None

        if self.corker is not None:
            self.corker.stop()
            self.corker = None

    def cork(self):
        """
        Insert read barrier to the queue of pending changes.
        Return transaction identificator.
        """

        with self.conn.cursor() as curs:
            curs.execute('SELECT cork() AS cork;')
            return int(curs.fetchone()[0])

    def poll(self):
        """
        Receive fresh changes from the database.
        """

        self.conn.poll()

        while len(self.conn.notifies) > 0:
            item = loads(self.conn.notifies.pop().payload)

            op   = item[0]
            txid = int(item[1])

            def flush():
                print 'flushing', self.changes

                # Notify listeners about new completed transaction.
                for callback in self.callbacks:
                    callback(self.changes)

                # Reset accumulated changes and transaction id.
                self.changes = []
                self.txid = txid

                # Unblock any waiting threads.
                if txid in self.transactions:
                    self.transactions.pop(txid).callback(txid)

            if self.txid is None:
                # First transaction should not trigger a flush.
                self.txid = txid

            if self.txid != txid:
                # Flush on txid change.
                flush()

            if op == 'u':
                # Accumulate changes.
                entity, action, pkey = item[2:5]
                payload = loads(item[-1])

                if action == 'DELETE':
                    change = (entity, payload[pkey], 'desired', None)
                else:
                    change = (entity, payload[pkey], 'desired', payload)

                self.changes.append(change)

            elif op == 'c':
                # Flush on every cork operation.
                flush()


# vim:set sw=4 ts=4 et:
