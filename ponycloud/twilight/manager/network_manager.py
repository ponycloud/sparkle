#!/usr/bin/python -tt

from ponycloud.twilight.network import *

class NetworkManager(object):
    """Manager Mixin that takes care of network configuration."""

    def __init__(self):
        # Network configuration object.
        self.networking = Networking()

        # Sequences for bond and bridge naming.
        self.bondseq = 0
        self.brseq = 0


    def network_event(self, action, ifname):
        """Sink for udev events related to networking."""

        if action == 'add':
            iface = self.networking[ifname]
            if isinstance(iface, Physical):
                if iface.hwaddr in self.model['nic']:
                    self.model['nic'].update_row(iface.hwaddr, 'current', {
                        'nic_name': ifname,
                    })

        row = self.model['nic'].one(nic_name=ifname)
        if row is not None:
            return self.nic_event(action, row)

        row = self.model['bond'].one(bond_name=ifname)
        if row is not None:
            return self.bond_event(action, row)

        row = self.model['nic_role'].one(vlan_name=ifname)
        if row is not None:
            return self.vlan_event(action, row)

        row = self.model['nic_role'].one(bridge_name=ifname)
        if row is not None:
            return self.bridge_event(action, row)


    def create_bond(self, uuid):
        """
        Make sure bond with given uuid exists.

        Returns True if newly created, False if it already existed.
        The newly created bond name is added to the current state of
        row matching the passed uuid (which should be valid bond pkey,
        by the way).
        """

        if self.model['bond'][uuid].get('bond_name') is not None:
            # According to current state the bond already exists.
            return False

        # We don't have interface for this row, create one.
        # No need to configure it right now, we'll get notified later.
        print 'creating bond pc-bond%i' % self.bondseq
        bond = Bond.create('pc-bond%i' % self.bondseq)
        self.bondseq += 1

        # And remember it was for this row.
        self.model['bond'].update_row(uuid, 'current', {
            'bond_name': bond.name,
        })

        return True


    def configure_bond(self, row):
        """Configure an existing bond interface to match desired state."""

        print 'configuring bond %s' % row['bond_name']

        # Get the configuration proxy object.
        bond = self.networking[row['bond_name']]

        # Bring the interface down in order to configure it.
        bond.state = 'down'

        # Configure the bond interface according to the desired state.
        for k, v in row.desired.items():
            if k in ('mode', 'lacp_rate', 'xmit_hash_policy'):
                if v is not None:
                    setattr(bond, k, v)

        # Bring it back up once everything is set.
        bond.state = 'up'


    def enslave_bond_interfaces(self, row):
        """
        Enslave present interfaces.

        All interfaces that refer to this bond in their desired state and
        are present in the system (meaning they have assigned nic_name in
        the current state) are enslaved to bond specified by given row.
        """

        # Get the configuration proxy object.
        bond = self.networking[row['bond_name']]

        # Add missing slaves.
        for slave in self.model['nic'].list(bond=row.pkey):
            slave_iface = slave.get('nic_name')
            if slave_iface is not None:
                if slave_iface not in bond.slaves:
                    print 'enslaving %s to bond %s' % (slave_iface, bond.name)
                    self.networking[slave_iface].state = 'down'
                    bond.slave_add(slave_iface)


    def nic_event(self, action, row):
        """Sink for physical interface events."""

        if action == 'add':
            if row.desired['bond'] is not None:
                # Create the bond.
                if not self.create_bond(row.desired['bond']):
                    # It was already there, do just the enslavement.
                    bond_row = self.model['bond'][row.desired['bond']]
                    self.bond_event('enslave', bond_row)

        elif action == 'remove':
            # Forget about the interface.
            self.model['nic'].update_row(row.pkey, 'current', {
                'nic_name': None,
            })


    def bond_event(self, action, row):
        """Sink for bond interface events."""

        # Get the network interface for configuration.
        bond = self.networking.get(row['bond_name'])

        if action == 'add':
            self.configure_bond(row)

        if action in ('add', 'enslave'):
            self.enslave_bond_interfaces(row)
        elif action == 'remove':
            # Forget the bond interface.
            self.model['bond'].update_row(row.pkey, 'current', {
                'bond_name': None,
            })


    def vlan_event(self, action, row):
        """Sink for nic_role/vlan events."""
        print 'vlan event', action, row.pkey


    def bridge_event(self, action, row):
        """Sink for nic_role/bridge events."""
        print 'bridge event', action, row.pkey


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
