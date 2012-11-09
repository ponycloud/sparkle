#!/usr/bin/python -tt

from iproute import ip

from iface import Interface
from sysfs import sys, proc

class Bond(Interface):
    """Wraps bonding interfaces."""

    @classmethod
    def create(cls, name):
        """Create bond interface with given name."""
        sys['class']['net']['bonding_masters'] = '+' + name
        return cls(name)


    @property
    def slaves(self):
        """List of enslaved interface names."""
        ifaces = []
        for fname in self.node:
            if fname.startswith('slave_'):
                ifaces.append(fname[6:])
        return ifaces


    def slave_add(self, slave):
        """Add slave interface by name."""
        self.node['bonding']['slaves'] = '+' + slave


    def slave_del(self, slave):
        """Remove slave interface by name."""
        self.node['bonding']['slaves'] = '-' + slave


    @property
    def ad_select(self):
        """
        803.ad aggregation selection logic

        Can be 'stable' (default), 'bandwidth', or 'count'.
        """
        return self.node['bonding']['ad_select'].split(' ')[0]


    @ad_select.setter
    def ad_select(self, value):
        self.node['bonding']['ad_select'] = value


    @property
    def downdelay(self):
        """Delay before considering link down, in milliseconds."""
        return self.node['bonding']['downdelay']


    @downdelay.setter
    def downdelay(self, value):
        self.node['bonding']['downdelay'] = value


    @property
    def updelay(self):
        """Delay before considering link up, in milliseconds."""
        return self.node['bonding']['updelay']


    @updelay.setter
    def updelay(self, value):
        self.node['bonding']['updelay'] = value


    @property
    def lacp_rate(self):
        """
        LACPDU tx rate to request from 802.3ad partner.

        Rate can be either "slow" or "fast".
        """
        return self.node['bonding']['lacp_rate'].split(' ')[0]


    @lacp_rate.setter
    def lacp_rate(self, value):
        self.node['bonding']['lacp_rate'] = value


    @property
    def miimon(self):
        """Link check interval in milliseconds."""
        return self.node['bonding']['miimon']


    @miimon.setter
    def miimon(self, value):
        self.node['bonding']['miimon'] = value


    @property
    def mode(self):
        """
        Mode of operation.

        Valid modes are "balance-rr", "active-backup", "balance-xor",
        "broadcast", "802.3ad", "balance-tlb", or "balance-alb".
        """
        return self.node['bonding']['mode'].split(' ')[0]


    @mode.setter
    def mode(self, value):
        self.node['bonding']['mode'] = value


    @property
    def xmit_hash_policy(self):
        """
        Hashing method for balance-xor and 802.3ad modes.

        Can be "layer2", "layer2+3", "layer3+4".
        """
        return self.node['bonding']['xmit_hash_policy'].split(' ')[0]


    @xmit_hash_policy.setter
    def xmit_hash_policy(self, value):
        self.node['bonding']['xmit_hash_policy'] = value


    def destroy(self):
        """Destroy the bonding interface."""
        sys['class']['net']['bonding_masters'] = '-' + self.name


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
