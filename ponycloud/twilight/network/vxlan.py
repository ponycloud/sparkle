#!/usr/bin/python -tt

from iproute import ip

from iface import Interface
from sysfs import sys

class VXLAN(Interface):
    """Wraps VXLAN tunnel interfaces."""

    @classmethod
    def create(cls, name, group, tag):
        """Create VXLAN tunnel with a tag and mcast group."""
        ip(['link', 'add', name, 'type', 'vxlan', 'id', str(tag), 'group', group])
        return cls(name)


    def destroy(self):
        """Destroy the VXLAN tunnel interface."""
        ip(['link', 'delete', self.name, 'type', 'vxlan'])


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
