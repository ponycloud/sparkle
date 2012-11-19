#!/usr/bin/python -tt

from twisted.internet import reactor
from pyudev import Context, Monitor, MonitorObserver

class UdevManager(object):
    """Manger mixin that takes care of udev event handling."""

    def __init__(self):
        # Create connection to udev.
        self.udev = Context()


    def start_udev_tasks(self):
        # Monitor interesting system events.
        self.monitor = Monitor.from_netlink(self.udev)
        self.observer = MonitorObserver(self.monitor, from_thread(self.udev_event))
        self.observer.start()


    def udev_event(self, action, device):
        """Handler for udev notifications."""

        if device.subsystem not in ('net', 'block'):
            # We are only interested in storage and network interfaces.
            return

        print 'udev event:', action, device.subsystem, device.sys_name

        if device.subsystem == 'net':
            return self.network_event(action, device.sys_name)


def from_thread(fn):
    """Returns function that forwards call from other thread."""
    return lambda *a, **kw: reactor.callFromThread(fn, *a, **kw)


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
