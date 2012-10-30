#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import task, reactor
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.internet.defer import Deferred

from ponycloud.common.util import uuidgen

class Manager(object):
    """
    The main application logic of Sparkle.
    """

    def __init__(self, router, api, db):
        """
        Stores the event sinks for later use.
        """
        self.db = db
        self.api = api
        self.router = router
        self.incarnation = uuidgen()

        # Dictionary with host informations.
        self.hosts = {}

        # Dictionary with desired and current configuration of all
        # managed entities.  Beware, potentially extremely large.
        self.config = {'children': {}}
    # /def __init__

    def start(self):
        """
        Registers periodic events.
        """
        print 'starting manager'

        # We need to configure relations between database tables.
        self.schedule_relate()
    # /def start

    def schedule_relate(self):
        """
        Schedules an asynchronous attempt to configure DB relations.

        If the relate fails, it is automatically retried every 15
        seconds until it succeeds.  Call only once.
        """

        print 'scheduling db relate'

        # Attempt the relate.
        d = deferToThread(self.api.relate, self.db)

        # Configure where to go from there.
        d.addCallbacks(lambda *a: self.schedule_db_dump(),
                       lambda *a: reactor.callLater(15, self.schedule_relate))
    # /def schedule_relate

    def schedule_db_dump(self):
        """
        Schedules an asynchronous attempt to dump all DB entities.

        If the attempt fails, it is automatically retried every 15
        seconds until it succeeds.  Call only once.
        """

        print 'scheduling db dump'
        print ' (not really, TODO)'

    # /def schedule_db_dump

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
    # /def twilight_presence

    def list_collection(self, path, keys, page=0):
        """
        Called from API to obtain list of collection items.
        """

        return {
            'type': 'collection',
            'path': '/'.join([x[0] for x in path]),
            'keys': keys,
        }
    # /def list_collection

    def get_entity(self, path, keys):
        """
        Called from API to obtain specific entity.
        """

        return {
            'type': 'entity',
            'path': '/'.join([x[0] for x in path]),
            'keys': keys,
        }
    # /def get_entity

# /class Manager

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
