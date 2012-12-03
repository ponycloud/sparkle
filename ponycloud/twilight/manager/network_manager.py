#!/usr/bin/python -tt

from twisted.internet import reactor
from ponycloud.twilight.network import *

class NetworkManager(object):
    """Manager Mixin that takes care of network configuration."""

    def __init__(self):
        # Network configuration object.
        self.networking = Networking()

        # Sequence for bond naming.
        self.bondseq = 0

        # Register event handlers that take care of configuration changes.
        for t in ('nic', 'bond', 'nic_role'):
            for ac in ('create', 'update', 'delete'):
                self.on_events([(ac, t)], getattr(self, '%s_%s' % (ac, t)))

        # And event handlers that take care of udev notifications.
        self.on_events([('add',    'net')], self.add_net)
        self.on_events([('change', 'net')], self.change_net)
        self.on_events([('remove', 'net')], self.remove_net)


    def poke_network_interface(self, row):
        """
        Poke interface(s) named in the specified row.

        The row can be one of `nic`, `bond` or `nic_role` for all it's
        known and present interfaces an `add` event is generated.
        """

        for key in ('nic_name', 'bond_name', 'vlan_name', 'bridge_name'):
            if row.get_current(key) is not None:
                if row.current[key] in self.networking:
                    self.add_net(list(self.udev.list_devices(**{
                        'subsystem': 'net',
                        'INTERFACE': row.current[key],
                    }))[0])


    def create_nic(self, nic_row):
        if nic_row.get_desired('bond') is None:
            # Do nothing if the NIC has no bond.
            return

        # Find the corresponding bond row.
        bond_row = self.model['bond'][nic_row.desired['bond']]

        # Make sure the bond exists, it will also take care of the enslavement.
        self._setup_bond(bond_row)


    def update_nic(self, row):
        if row.get_current('nic_name') is None:
            # Do not bother if the interface does not exist.
            return

        if row.get_desired('bond') != row.get_current('bond'):
            # Act as if the NIC have been deconfigured.
            self.delete_nic(row)

        # Enslave NIC to the correct bond and everything...
        self.create_nic(row)


    def delete_nic(self, row):
        if row.get_current('bond') is None:
            # Not a bond member, there is nothing special to do.
            return

        # Find corresponding bond row.
        bond_row = self.model['bond'][row.current['bond']]

        # Get the bond configuration proxy.
        bond = self.networking[bond_row.current['bond_name']]

        if row.get_current('nic_name') is not None:
            if row.current['nic_name'] in bond.slaves:
                # Remove the NIC as a bond slave.
                bond.slave_del(row.current['nic_name'])

            # Note down that this NIC no longer belongs to the bond.
            self.apply_change('nic', row.pkey, 'current', {
                'bond': None,
            })

            # If the bond have no more slaves, delete it completely.
            if 0 == len(bond.slaves):
                self._destroy_bond(bond_row)


    def create_bond(self, bond_row):
        for i in xrange(9999):
            if self.bondseq > 9999:
                self.bondseq = 0
            name = 'pc-%i' % self.bondseq
            self.bondseq += 1

            if name not in self.networking:
                break

        # Just die if we have more than 10'000 bonds.
        assert name not in self.networking

        # Update current state.
        self.apply_change('bond', bond_row.pkey, 'current', {
            'bond_name': name,
            'bridge_name': name + 'b',
            'state': 'missing',
        })
        self.bondseq += 1


    def update_bond(self, row):
        self.delete_bond(row)
        self.create_bond(row)


    def delete_bond(self, row):
        # Destroy the bond and everything on top of it.
        self._destroy_bond(row)


    def create_nic_role(self, role_row):
        # Row for role's bond.
        bond_row = self.model['bond'][role_row.desired['bond']]

        # If we do not have to create a VLAN interface and another bridge.
        if role_row.get_desired('vlan_id') is None:
            address = None
            if bond_row.get_current('state') == 'present':
                address = role_row.desired['address']
                self._setup_nic_role(role_row)

            # We might be done here.
            self.apply_change('nic_role', role_row.pkey, 'current', {
                'bond': None,
                'vlan_name': None,
                'bridge_name': None,
                'interface': bond_row.current['bridge_name'],
                'address': address,
                'state': bond_row.get_current('state', 'missing'),
            })

        else:
            name = '%s.%i' % (bond_row.current['bridge_name'],
                              role_row.desired['vlan_id'])
            self.apply_change('nic_role', role_row.pkey, 'current', {
                'bond': None,
                'vlan_name': name,
                'bridge_name': name + 'b',
                'interface': name + 'b',
                'address': None,
                'state': 'missing',
            })

        # Now, if the bond is already present, setup the role.
        if bond_row.get_current('state') == 'present':
            self._setup_nic_role(role_row)


    def update_nic_role(self, row):
        self.delete_nic_role(row)
        self.create_nic_role(row)


    def delete_nic_role(self, row):
        self._destroy_nic_role(row)


    def add_net(self, dev):
        # Get the network device configuration proxy.
        iface = self.networking[dev.sys_name]

        if isinstance(iface, Physical):
            # Pair up physical interfaces in current state and note that
            # it is not yet member of any bond.
            self.apply_change('nic', iface.hwaddr, 'current', {
                'nic_name': dev.sys_name,
                'bond': None,
            })

            # Get the (now definitely existing) NIC row.
            nic_row = self.model['nic'][iface.hwaddr]

            # Check whether the NIC is to be enslaved to a bond.
            if nic_row.get_desired('bond') is not None:
                bond_row = self.model['bond'][nic_row.desired['bond']]

                # Make sure the bond is present. This will also take care
                # of the enslavement of our new NIC.
                self._setup_bond(bond_row)


    def change_net(self, dev):
        pass


    def remove_net(self, dev):
        rows = self.model['nic'].list(nic_name=dev.sys_name)
        if len(rows) > 0:
            row = rows.pop()

            # Remember last bond of the NIC.
            current_bond = row.get_current('bond')

            # Drop current state, since the NIC is gone.
            self.apply_change('nic', row.pkey, 'current', None)

            if current_bond is not None:
                # NIC have been part of a bond, we need to recreate it in
                # order to fix possibly broken hardware addresses. :-(

                # Get the old bond info.
                bond_row = self.model['bond'][current_bond]
                bond = self.networking[row.desired['bond']]

                # Get current slaves of that bond, we do nothing if this
                # NIC was the only slave.
                slaves = bond.slaves

                # Get rid of the old bond.
                self._destroy_bond(bond_row)

                if len(slaves) > 0:
                    # And since there were some other slaves, we recreate
                    # the bond again with just them. This will properly
                    # cascade and bring correct hardware addresses along
                    # the way.
                    self._setup_bond(bond_row)


    def _setup_bond(self, row):
        if row.current is None:
            # Do nothing if we do not yet have any interface names allocated.
            return

        if row.current['bond_name'] not in self.networking:
            # Create the bond interface and bring it down for configuration.
            bond = Bond.create(row.current['bond_name'])
            bond.state = 'down'

            # Set it's parameters, mode first.
            for k in ('mode', 'xmit_hash_policy', 'lacp_mode'):
                if row.get_desired(k) is not None:
                    setattr(bond, k, row.desired[k])

            # Now we can bring the bond up.
            bond.state = 'up'
        else:
            bond = self.networking[row.current['bond_name']]

        # Enslave all NICs that should be enslaved to it and are present.
        for nic_row in self.model['nic'].list(bond=row.pkey):
            if nic_row.get_current('nic_name') is None or \
               nic_row.get_desired('bond') != row.pkey:
                # Skip NICs that are not present.
                continue

            # Get NIC configuration proxy.
            nic = self.networking[nic_row.current['nic_name']]

            if not nic.name in bond.slaves:
                # Enslave the NIC to the bond.
                nic.state = 'down'
                bond.slave_add(nic.name)

                # Write down that the NIC is now enslaved to this bond.
                self.apply_change('nic', nic_row.pkey, 'current', {
                    'bond': row.pkey,
                    'nic_name': nic.name,
                })

        if 0 == len(bond.slaves):
            # We don't have any slaves, abort!
            bond.destroy()
            return

        if row.current['bridge_name'] not in self.networking:
            # Now, we can create a bridge, configure it and bring it up.
            bridge = Bridge.create(row.current['bridge_name'])
            bridge.forward_delay = 0
            bridge.state = 'up'

            # And plug the bond to that bridge.
            bridge.port_add(bond.name)

            # Update current state to reflect that this bond is present.
            self.apply_change('bond', row.pkey, 'current', {
                'state': 'present',
            })

        # Finally, create the roles that should be on this bond.
        for role in self.model['nic_role'].list(bond=row.pkey):
            if role.get_desired('bond') == row.pkey:
                self._setup_nic_role(role)


    def _destroy_bond(self, row):
        if row.get_current('state') == 'present':
            # Find interface configuration proxies.
            bridge = self.networking[row.current['bridge_name']]
            bond = self.networking[row.current['bond_name']]

            # Remove all remaining bridge ports.
            for port in bridge.ports:
                bridge.port_del(port)

            # Get rid of the bridge.
            bridge.state = 'down'
            bridge.destroy()

            # Get rid of the bond.
            bond.state = 'down'
            bond.destroy()

            self.apply_change('bond', row.pkey, 'current', None)

        # Destroy all roles defined on the bond as well.
        for role in self.model['nic_role'].list(bond=row.pkey):
            if role.get_current('bond') == row.pkey:
                self._destroy_nic_role(role)


    def _setup_nic_role(self, row):
        if row.current is None:
            # Do nothing if we do not yet have any interface names allocated.
            return

        # Get the bond the role is defined on.
        bond_row = self.model['bond'][row.desired['bond']]
        bond_bridge = self.networking[bond_row.current['bridge_name']]

        # Role may use VLAN, if it does, we need to create whole bunch
        # of interface to support it.
        if row.get_desired('vlan_id') is not None:
            if row.current['bridge_name'] not in self.networking:
                bridge = Bridge.create(row.current['bridge_name'])
                bridge.forward_delay = 0
                bridge.state = 'up'
            else:
                bridge = self.networking[row.current['bridge_name']]

            if row.current['vlan_name'] not in self.networking:
                vlan = VLAN.create(bond_bridge.name, row.desired['vlan_id'])
                vlan.state = 'up'
            else:
                vlan = self.networking[row.current['vlan_name']]

            if vlan.name not in bridge.ports:
                bridge.port_add(vlan.name)

        if row.get_desired('address') is not None:
            # Assign the address if not yet present.
            iface = self.networking[row.current['interface']]
            if row.desired['address'] not in (iface.inet + iface.inet6):
                iface.addr_add(row.desired['address'])

        self.apply_change('nic_role', row.pkey, 'current', {
            'bond': bond_row.pkey,
            'address': row.get_desired('address'),
            'state': 'present',
        })


    def _destroy_nic_role(self, row):
        if row.get_current('state') == 'present':
            # Deconfigure the address if it's not shared.
            if row.get_current('address') is not None:
                roles = self.model['nic_role']\
                        .list(bond=row.current['bond'],
                              address=row.current['address'])

                if 1 == len(roles):
                    iface = self.networking[row.current['interface']]
                    iface.addr_del(row.current['address'])

            # Find interface configuration proxies.
            bridge = self.networking[row.current['bridge_name']]
            vlan = self.networking[row.current['vlan_name']]

            # Remove them if they are not shared.
            roles = self.model['nic_role'].list(bridge_name=bridge.name)
            if 1 == len(roles):
                # Remove bridge ports.
                for port in bridge.ports:
                    bridge.port_del(port)

                # Get rid of the bridge.
                bridge.state = 'down'
                bridge.destroy()

                # Get rid of the vlan.
                vlan.state = 'down'
                vlan.destroy()

            # Remove the role from current state.
            self.apply_change('nic_role', row.pkey, 'current', None)


    def network_cleanup(self):
        """
        Clean up network interfaces.

        This routine is normally not really useful, but during testing it
        cleans up most of the mess Twilight caused to the system, which
        can be quite helpful.
        """

        for t in ('nic_role', 'bond'):
            for row in self.model[t].itervalues():
                for key in ('bridge_name', 'vlan_name', 'bond_name'):
                    if row.get_current(key) is not None:
                        if row.current[key] in self.networking:
                            iface = self.networking[row.current[key]]
                            iface.state = 'down'
                            iface.destroy()


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
