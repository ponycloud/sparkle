#!/usr/bin/python -tt

__all__ = ['Interface']

from sysfs import sys, proc, Node
from iproute import ip

from os.path import basename

import re

class Interface(object):
    """
    Wrapper for generic network interface operations.
    """

    def __init__(self, name):
        """Stores the interface name for further operations."""
        self.name = name
        self.node = sys['class']['net'][name]


    @property
    def index(self):
        """
        Interface index.

        Used to find out about child/parent interface relations and
        to indentify interface when it's name is not stable.
        """
        return self.node['ifindex']


    @property
    def link(self):
        return self.node['iflink']


    @property
    def state(self):
        """
        Returns either 'up' or 'down', depending on interface state.
        Setting to one of these values immediately changes interface state.
        """
        flags = ip(['link', 'show', 'dev', self.name])[self.name]['flags']
        return 'up' if 'UP' in flags else 'down'


    @state.setter
    def state(self, value):
        if value not in ('up', 'down'):
            raise ValueError('link can be either "up" or "down"')
        ip(['link', 'set', value, 'dev', self.name])


    @property
    def hwaddr(self):
        """
        Returns or changes interface hardware address.

        If the interface is enslaved by a bond, the permanent HW address
        is returned instead of the address assigned by the bond.
        """
        if 'master' not in self.node:
            return self.node['address']

        bond = basename(self.node['master'].path)
        data = proc['net']['bonding'][bond]
        m = dict(re.findall('Slave Interface: ([^\n]+)\n.*?addr: ([^\n]+)', data, re.S))
        return m[self.name]


    @hwaddr.setter
    def hwaddr(self, value):
        if value != self.hwaddr:
            ip(['link', 'set', 'address', value, 'dev', self.name])


    @property
    def mtu(self):
        """Returns or changes MTU of the interface."""
        return self.node['mtu']


    @property
    def parent(self):
        """Name of the parent interface. None if no parent."""
        link = self.link

        if self.index == link:
            return None

        for ifname in sys['class']['net']:
            if isinstance(sys['class']['net'][ifname], Node):
                ifindex = sys['class']['net'][ifname]['ifindex']
                if ifindex == link:
                    return ifname

        # Very improbable, but possible due to races.
        raise LookupError('parent not found')


    @mtu.setter
    def mtu(self, value):
        ip(['link', 'set', 'mtu', str(value)])


    @property
    def inet(self):
        """List of interfaces IPv4 addresses."""
        inet = ip(['addr', 'show', 'dev', self.name])[self.name]\
                .get('inet', [])
        return [x['address'] for x in inet]


    @property
    def inet6(self):
        """List of interface IPv6 addresses."""
        inet6 = ip(['addr', 'show', 'dev', self.name])[self.name]\
                .get('inet6', [])
        return [x['address'] for x in inet6]


    def addr_add(self, addr):
        """
        Adds another IPv4 or IPv6 address to the interface.
        The address string should include network prefix.
        """
        ip(['addr', 'add', addr, 'dev', self.name])


    def addr_del(self, addr):
        """
        Removes IPv4 or IPv6 address from the interface.
        You should use the canonical form with prefix.
        """
        ip(['addr', 'del', addr, 'dev', self.name])


# /class Interface

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
