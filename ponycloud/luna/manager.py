#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Manager']

from twisted.internet import task, reactor
from ponycloud.common.util import uuidgen

class Manager(object):
    """
    The main application logic of Luna.
    """

    def __init__(self, sparkle):
        """
        Stores the Sparkle connection for later use.
        """
        self.sparkle = sparkle
        self.incarnation = uuidgen()

    def start(self):
        """
        Starts regular heartbeat to Sparkle.
        """
        task.LoopingCall(self.presence).start(10.0)

    def presence(self):
        """
        Sends presence message to Sparkle.
        """
        self.sparkle.send({
            'event': 'luna-presence',
            'incarnation': self.incarnation,
        })

# vim:set sw=4 ts=4 et:
