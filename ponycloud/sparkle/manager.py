#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import task, reactor
from ponycloud.common.util import uuidgen

class Manager(object):
    """
    The main application logic of Sparkle.
    """

    def __init__(self, router):
        """
        Stores the event sinks for later use.
        """
        self.router = router
        self.incarnation = uuidgen()

        # Dictionary with host informations.
        self.hosts = {}

        # Dictionary with desired and current configuration of all
        # managed entities.  Beware, potentially extremely large.
        self.config = {'children': {}}

    def start(self):
        """
        Registers periodic events.
        """
        print 'starting manager'

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

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
