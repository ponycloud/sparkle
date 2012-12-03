#!/usr/bin/python -tt

__all__ = ['Networking', 'Bridge', 'Bond', 'VLAN', 'VXLAN', 'Physical']

from bond import *
from bridge import *
from iface import *
from vlan import *
from vxlan import *
from physical import *

from sysfs import sys, proc, Node


def bond_flags(bond):
    """Filters relevant flags from bond definition."""
    FLAGS = set(['mode', 'lacp_rate', 'xmit_hash_policy'])
    return {k: v for k in bond if k in FLAGS and v is not None}


class Networking(object):
    """
    Wrapper for system networking configuration.
    """

    def __iter__(self):
        """Iterates over names of system network interfaces."""
        return iter([ifname for ifname in sys['class']['net']
                            if isinstance(sys['class']['net'][ifname], Node)])


    def find_physical_by_mac(self, mac):
        """Returns physical interface with specified MAC address."""
        for dev in self:
            if isinstance(dev, Physical):
                if dev.hwaddr == mac:
                    return dev
        return None


    def __getitem__(self, name):
        """Retrieves interface proxy object, guessing interface type."""

        if name not in sys['class']['net']:
            raise KeyError('no such network interface')

        if 'device' in sys['class']['net'][name]:
            return Physical(name)

        if sys['class']['net'][name]['bonding']:
            return Bond(name)

        if sys['class']['net'][name]['bridge']:
            return Bridge(name)

        if 'DEVTYPE=vxlan' in sys['class']['net'][name]['uevent']:
            return VXLAN(name)

        if proc['net']['vlan'] and proc['net']['vlan'][name]:
            return VLAN(name)

        if name.startswith('dummy'):
            return Physical(name)

        return Interface(name)


    def get(self, name, default=None):
        """Return the interface if it exists, default otherwise."""
        try:
            return self[name]
        except KeyError:
            return default


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
