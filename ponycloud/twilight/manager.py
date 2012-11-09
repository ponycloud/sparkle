#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import reactor, task

from ponycloud.common.util import uuidgen

from ponyvirt import Hypervisor

import network
import pyudev


def from_thread(fn):
    """Returns function that forwards call from other thread."""
    return lambda *a, **kw: reactor.callFromThread(fn, *a, **kw)


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

        print 'connecting to udev'
        self.udev = pyudev.Context()
        self.monitor = pyudev.Monitor.from_netlink(self.udev)
        self.observer = pyudev.MonitorObserver(self.monitor, \
                                               from_thread(self.on_udev_event))
        self.observer.start()


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


    def on_udev_event(self, action, device):
        """Handler of background udev notifications."""
        if device.subsystem not in ('net', 'block'):
            return

        print 'udev event:', action, device.subsystem, device.sys_name


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
