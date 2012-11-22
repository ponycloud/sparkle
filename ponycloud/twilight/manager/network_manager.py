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
                self.apply_changes([('nic', iface.hwaddr, 'current', {
                    'nic_name': ifname,
                })])

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


    def create_bond(self, row):
        """
        Make sure specifid bond exists.

        Returns True if newly created, False if it already existed.
        The newly created bond name is added to the current state of
        row matching the passed uuid (which should be valid bond pkey,
        by the way).
        """

        if row.get('bond_name') is not None:
            # According to current state the bond already exists.
            return False

        # We don't have interface for this row, create one.
        # No need to configure it right now, we'll get notified later.
        print 'creating bond pc-bond%i' % self.bondseq
        bond = Bond.create('pc-bond%i' % self.bondseq)
        self.bondseq += 1

        # And remember it was for this row.
        self.apply_changes([('bond', row.pkey, 'current', {
            'bond_name': bond.name,
            'bond_configured': False,
        })])

        return True


    def configure_bond(self, row):
        """Apply bond configuration, such as mode."""

        # Get the configuration proxy.
        bond = self.networking[row['bond_name']]

        # Bring the bond down to configure it.
        bond.state = 'down'

        # Set various bond flags.
        for k in ('mode', 'lacp_rate', 'xmit_hash_policy'):
            if row.get(k) is not None:
                print '  * %s.%s = %s' % (bond.name, k, row[k])
                setattr(bond, k, row[k])

        # Bring the bond back up to be able to enslave interfaces to it.
        bond.state = 'up'

        # It is configured now.
        self.apply_changes([('bond', row.pkey, 'current', {
            'bond_configured': True,
        })])


    def remove_bond(self, row):
        """Remove the bond interface."""

        if row.get('bond_name') is not None:
            if row['bond_name'] in self.networking:
                print 'destroy bond %s' % row['bond_name']
                self.networking[row['bond_name']].destroy()

        # And remember it is no longer there.
        self.apply_changes([('bond', row.pkey, 'current', {
            'bond_name': None,
            'bond_configured': False,
        })])


    def enslave(self, row):
        """
        Enslave NIC to it's bond.

        If the bond does not exist or is not yet fully configured,
        do not try to do anything.
        """

        if row.get('nic_name') is not None and row.get('bond') is not None:
            bond_row = self.model['bond'][row['bond']]

            if not bond_row.get('bond_configured'):
                # Do not enslave if the bond is not yet configured.
                return

            if bond_row.get('bond_name') is not None:
                iface = self.networking[row['nic_name']]
                bond = self.networking[bond_row['bond_name']]

                if iface.name not in bond.slaves:
                    print 'enslaving %s to %s' % (iface.name, bond.name)
                    iface.state = 'down'
                    bond.slave_add(iface.name)


    def unenslave(self, row):
        """Un-enslaves nic from it's configured bond."""

        if row.get('nic_name') is not None and row.get('bond') is not None:
            bond_row = self.model['bond'][row['bond']]
            if bond_row.get('bond_name') is not None:
                bond = self.networking[bond_row['bond_name']]
                if row['nic_name'] in bond.slaves:
                    print 'un-enslaving %s from %s' \
                            % (row['nic_name'], bond.name)
                    bond.slave_del(row['nic_name'])


    def nic_event(self, action, row):
        """Sink for physical interface events."""

        print '>>> nic event', action, row.pkey

        if action in ('add', 'configure'):
            if row.get('bond') is not None:
                bond_row = self.model['bond'][row['bond']]
                if not self.create_bond(bond_row):
                    self.enslave(row)

        elif action == 'remove':
            self.apply_changes([('nic', row.pkey, 'current', {
                'nic_name': None,
            })])

        elif action == 'deconfigure':
            self.unenslave(row)


    def bond_event(self, action, row):
        """Sink for bond interface events."""

        print '>>> bond event', action, row.pkey

        if action == 'add':
            # Apply bond configuration.
            self.configure_bond(row)

            # Enslave it's interfaces.
            for nic in self.model['nic'].list(bond=row.pkey):
                self.enslave(nic)

        elif action == 'configure':
            self.create_bond(row)

        elif action in ('remove', 'deconfigure'):
            self.remove_bond(row)


    def vlan_event(self, action, row):
        """Sink for nic_role/vlan events."""
        print '>>> vlan event', action, row.pkey


    def bridge_event(self, action, row):
        """Sink for nic_role/bridge events."""
        print '>>> bridge event', action, row.pkey


    def address_event(self, action, row):
        """Sink for nic_role/address events."""
        print '>>> address event', action, row.pkey


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
