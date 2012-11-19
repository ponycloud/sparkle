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
        """Make sure bond with given uuid exists."""

        if self.model['bond'][uuid].get('bond_name') is not None:
            # According to current state the bond already exists.
            return False

        # We don't have interface for this row, create one.
        # No need to configure it right now, we'll get notified later.
        print 'create bond pc-bond%i' % self.bondseq
        bond = Bond.create('pc-bond%i' % self.bondseq)
        self.bondseq += 1

        # And remember it was for this row.
        self.model['bond'].update_row(uuid, 'current', {
            'bond_name': bond.name,
        })

        return True


    def configure_bond(self, row):
        """Configures an existing bond interface to match desired state."""

        # Get the configuration proxy object.
        bond = self.networking[row['bond_name']]

        # Bring the interface down in order to configure it.
        bond.state = 'down'

        # Configure the bond interface according to the desired state.
        for k, v in row.desired.items():
            if k in ('mode', 'lacp_rate', 'xmit_hash_policy'):
                if v is not None:
                    print 'setting %s.%s = %s' % (bond.name, k, v)
                    setattr(bond, k, v)

        # Bring it back up once everything is set.
        bond.state = 'up'


    def enslave_bond_interfaces(self, row):
        """Enslaves present interfaces that are to be enslaved by this bond."""

        # Get the configuration proxy object.
        bond = self.networking[row['bond_name']]

        # Add missing slaves.
        for slave in self.model['nic'].list(bond=row.pkey):
            slave_iface = slave.get('nic_name')
            if slave_iface is not None:
                if slave_iface not in bond.slaves:
                    print 'enslave %s %s' % (bond.name, slave_iface)
                    self.networking[slave_iface].state = 'down'
                    bond.slave_add(slave_iface)


    def nic_event(self, action, row):
        print 'nic event', action, row.pkey

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
        print 'bond event', action, row.pkey

        # Get the network interface for configuration.
        bond = self.networking.get(row['bond_name'])

        if action == 'add':
            self.configure_bond(row)

        if action in ('add', 'enslave'):
            self.enslave_bond_interfaces(row)

        if action == 'remove':
            # Forget the bond interface.
            self.model['bond'].update_row(row.pkey, 'current', {
                'bond_name': None,
            })


    def vlan_event(self, action, row):
        print 'vlan event', action, row.pkey


    def bridge_event(self, action, row):
        print 'bridge event', action, row.pkey


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
