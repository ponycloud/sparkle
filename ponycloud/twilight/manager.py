#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import reactor, task
from ponycloud.common.util import uuidgen

from ponyvirt import Hypervisor

class Manager(object):
    """
    The main application logic of Twilight.
    """

    def __init__(self, sparkle):
        """
        Stores the Sparkle connection for later use and connects to libvirt.
        """
        self.sparkle = sparkle

        print 'connecting to libvirt'
        self.virt = Hypervisor()
        self.uuid = self.virt.sysinfo['system']['uuid'].lower()
        self.incarnation = uuidgen()

    def start(self):
        """
        Perform the startup routine.

        It consists of two simple tasks:

         * Proactively send host info.
         * Start 15s cycle of heartbeat messages.
        """
        print 'starting manager'
        self.send_host_info()
        task.LoopingCall(self.presence).start(15.0)

    def send_host_info(self):
        """
        Sends system info and hypervisor capabilities to Sparkle.
        """

        print 'sending host info'
        self.sparkle.send({
            'event': 'host-info',
            'uuid': self.uuid,
            'incarnation': self.incarnation,
            'sysinfo': self.virt.sysinfo,
            'capabilities': self.virt.capabilities,
        })

    def presence(self):
        """
        Sends notification about our presence and our state.

        Normally triggered every 15 seconds. Missing this report
        by too long will lead to node eviction and immediate fencing.
        """
        self.sparkle.send({
            'event': 'twilight-presence',
            'uuid': self.uuid,
            'incarnation': self.incarnation,
        })


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
