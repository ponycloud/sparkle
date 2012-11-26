#!/usr/bin/python -tt

from twisted.internet import reactor
from pyudev import Context, Monitor, MonitorObserver


def from_thread(fn):
    """Returns function that forwards call from other thread."""
    return lambda *a, **kw: reactor.callFromThread(fn, *a, **kw)


class UdevManager(object):
    """Manger mixin that takes care of udev event handling."""

    def __init__(self):
        # Create connection to udev.
        self.udev = Context()


    def start_udev_tasks(self):
        """Start monitoring system devices using udev."""

        def udev_handler(action, device):
            if device.subsystem in ('net', 'block'):
                self.raise_event((action, device.subsystem), device)
                self.raise_event((action, device.subsystem, device.sys_name), device)

        # Monitor interesting system events.
        self.monitor = Monitor.from_netlink(self.udev)
        self.observer = MonitorObserver(self.monitor, from_thread(udev_handler))
        self.observer.start()

        # Trigger missed events.
        reactor.callLater(0, self.raise_missed_udev_events)


    def raise_missed_udev_events(self):
        """
        Raise events for all present net and block devices.

        This is intended to be executed right after we subscribe
        to regular udev event notifications to get up to speed with
        current state of the system.

        Raised events are:
            ('add', subsystem)            -> device
            ('add', subsystem, sys_name)  -> device
        """

        for subsystem in ('net', 'block'):
            for device in self.udev.list_devices(subsystem=subsystem):
                self.raise_event(('add', subsystem), device)
                self.raise_event(('add', subsystem, device.sys_name), device)


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
