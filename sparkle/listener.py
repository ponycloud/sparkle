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
        self.conn = psycopg2.connect(str(conn_string))
        self.listener = False
        self.callbacks = []

    def listen(self, interval=0.2):
        """
        Start listening and passing the results
        to callback after a certain interval passes
        """
        self._start()
        self.listener = task.LoopingCall(self.poll)
        self.listener.start(interval)

    def add_callback(self, callback):
        """
        Register another callback which should be fired
        on any event from db
        """
        if hasattr(callback, '__call__'):
            self.callbacks.append(callback)

    def stop(self):
        if self.listener:
            self.listener.stop()
            self.callback = lambda *args: None

    def _start(self):
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with self.conn.cursor() as curs:
            curs.execute("LISTEN changelog;")

    def poll(self):
            data = []
            self.conn.poll()

            while self.conn.notifies:
                item = loads(self.conn.notifies.pop().payload)
                entity, action, pkey = item[:3]
                payload = loads(item[-1])
                if action == 'DELETE':
                    data.append((entity, payload[pkey], 'desired', None))
                else:
                    data.append((entity, payload[pkey], 'desired', payload))

            if data:
                for callback in self.callbacks:
                    callback(data)

# vim:set sw=4 ts=4 et:
