#!/usr/bin/python -tt

from brctl import brctl
from iproute import ip

from iface import Interface
from sysfs import sys, proc, Node

class Bridge(Interface):
    """Wraps bridge interfaces."""

    @classmethod
    def create(cls, name):
        """Create new bridge with specified name."""
        ip(['link', 'add', name, 'type', 'bridge'])
        return cls(name)


    @property
    def ports(self):
        """List of bridge port interface names."""
        return list(self.node['brif'])


    @property
    def stp(self):
        """
        Spanning Tree Protocol

        True/False when enabled/disabled, changes are immediately
        applied to the bridge.
        """
        return 1 == self.node['bridge']['stp_state']


    @stp.setter
    def stp(self, value):
        self.node['bridge']['stp_state'] = 1 if value else 0


    @property
    def forward_delay(self):
        """
        Forwarding Delay

        Amount in centiseconds (hundredths of seconds).
        """
        fd = self.node['bridge']['forward_delay']
        if fd > 0:
            return fd + 1
        return 0


    @forward_delay.setter
    def forward_delay(self, value):
        self.node['bridge']['forward_delay'] = value


    def port_add(self, port):
        """Add another interface to the bridge."""
        brctl(['addif', self.name, port])


    def port_del(self, port):
        """Remove interface from the bridge."""
        brctl(['delif', self.name, port])


    def destroy(self):
        """Destroy the bridge."""
        ip(['link', 'delete', self.name, 'type', 'bridge'])


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
