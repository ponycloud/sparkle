#!/usr/bin/python -tt

from iproute import ip

from iface import Interface
from sysfs import sys, proc

import re

class VLAN(Interface):
    """Wraps VLAN interfaces."""

    @classmethod
    def create(cls, parent, tag):
        """Create VLAN-tagged interface on specified parent."""
        name = '%s.%i' % (parent, tag)
        ip(['link', 'add', name, 'link', parent,
                                 'type', 'vlan', 'id', str(tag)])
        return cls(name)


    @property
    def tag(self):
        """VLAN tag"""
        return int(re.findall('VID: ([0-9]+)', proc['net']['vlan'][self.name])[0])


    def destroy(self):
        """Destroy the VLAN interface."""
        ip(['link', 'delete', self.name, 'type', 'vlan'])


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
