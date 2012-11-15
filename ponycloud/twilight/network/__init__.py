#!/usr/bin/python -tt

__all__ = ['Networking']

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

        if proc['net']['vlan'][name]:
            return VLAN(name)

        return Interface(name)


    def create_recipe(self, configuration):
        """
        Creates a recipe to configure or unconfigure the network.
        The recipe is something like::
            [('bond', 'pc-bond0'),
             ('enslave', 'pc-bond0', 'eth0'),
             ('enslave', 'pc-bond0', 'eth1'),
             ('bridge', 'pc-br0'),
             ('port', 'pc-br0', 'pc-bond0'),
             ('role', u'management', 'pc-br0'),
             ('bridge', 'pc-br1'),
             ('vlan', 'pc-br0.101', 'pc-br0', 101),
             ('port', 'pc-br1', 'pc-br0.101'),
             ('role', u'core', 'pc-br1'),
             ('addr', 'pc-br0', '192.168.102.22/24'),
             ('role', u'virtual', 'pc-br0'),
             ('addr', 'pc-br0', '192.168.103.22/24'),
             ('role', u'storage', 'pc-br0')]
        """
        # Sequences for bond and bridge numbering.
        bondseq = 0
        brseq = 0

        # Filtered input items.
        in_nics  = [item[1] for item in configuration if item[0] == 'nic']
        in_bonds = [item[1] for item in configuration if item[0] == 'bond']
        in_roles = [item[1] for item in configuration if item[0] == 'nic_role']

        # Remembered interfaces used for input bonds, vlans and nics.
        # They can be overriden be bridges or nics, except for nics
        # themselves, which are only used during the bond construction.
        nics  = {i.hwaddr: i.name for i in [self[x] for x in self]
                                        if type(i) is Physical}
        bonds = {}
        vlans = {}

        # This is where changelog will end up.
        out = []

        # First of all, bonds are assembled from raw interfaces.
        for bond in in_bonds:
            name = 'pc-bond%i' % bondseq

            if bond['mode'] == 'active-backup':
                if 1 == len([1 for nic in in_nics if nic['bond'] == bond['uuid']]):
                    #
                    # In the special case of active-backup bond with only one
                    # interface, we do not create a bond at all.  This saves us
                    # some CPU cycles we just don't have to spend.
                    #
                    # Make sure we do not crash if the only configured interface
                    # cannot be found, fall back to creating an empty bond instead.
                    #
                    if nic['hwaddr'] in nics:
                        bonds[bond['uuid']] = nics[nic['hwaddr']]
                        continue

            # Produce the bond itself.
            out.append(('bond', name, bond_flags(bond)))
            bonds[bond['uuid']] = name
            bondseq += 1

            # Enslave all configured interfaces that are present.
            # Again, if the NIC is missing, ignore it.  It will
            # reflect in the current state, which is good enough.
            for nic in in_nics:
                if nic['bond'] == bond['uuid']:
                    if nic['hwaddr'] in nics:
                        out.append(('enslave', name, nics[nic['hwaddr']]))

        # Second step is to apply the two roles that use bridges instead
        # of just IP addresses.
        for role in in_roles:
            if role['role'] not in ('management', 'core'):
                continue

            # Bail out quickly if the bridge have already been created.
            vlan_key = (role['bond'], role['vlan_id'])
            if vlan_key in vlans:
                out.append(('role', role['role'], vlans[vlan_key]))
                continue

            # If not, create the bridge itself.
            name = 'pc-br%i' % brseq
            out.append(('bridge', name))
            brseq += 1

            # Remember the "(bond, vlan_id) => this new bridge" mapping,
            # it will prevent creation of second identical bridge in some
            # cases and will also cause proper placement of addresses atop
            # of this combination (bridge eats packets from all ports).
            vlans[vlan_key] = name

            if role['vlan_id'] is None:
                # When no VLAN tagging is required, plug the bond to the bridge
                # directly and make the bridge pose as the bond from now on.
                out.append(('port', name, bonds[role['bond']]))
                bonds[role['bond']] = name
            else:
                # When VLAN tagging is perfomed, create a vlan interface,
                # plug that into the bridge and be do not pose as anything.
                vlan = '%s.%i' % (bonds[role['bond']], role['vlan_id'])
                out.append(('vlan', vlan, bonds[role['bond']], role['vlan_id']))
                out.append(('port', name, vlan))

            # Do not forget to assign the role to this interface.
            # These two actually matters since instances use this into
            # to find bridges for their vnics.
            out.append(('role', role['role'], name))

        # Third step is to process all remaining roles.
        # All of these are there to assign some IP addresses.
        for role in in_roles:
            if role['role'] in ('management', 'core'):
                continue

            # Bail out quickly if vlan/bridge have already been created.
            vlan_key = (role['bond'], role['vlan_id'])
            if vlan_key in vlans:
                out.append(('addr', vlans[vlan_key], role['address']))
                out.append(('role', role['role'], vlans[vlan_key]))
                continue

            if role['vlan_id'] is None:
                # No tagging required, assign address, remember that there
                # have been no vlan for future reference and get out.
                out.append(('addr', bonds[role['bond']], role['address']))
                vlans[(vlan_key)] = bonds[role['bond']]
            else:
                # Create vlan interface, this time with no bridge -- just
                # slap the IP address on it and be done.
                vlan = '%s.%i' % (bonds[role['bond']], role['vlan_id'])
                vlans[(vlan_key)] = vlan
                out.append(('vlan', vlan, bonds[role['bond']], role['vlan_id']))
                out.append(('addr', vlan, role['address']))

            # And as always, assign the role.
            out.append(('role', role['role'], vlans[vlan_key]))
        return out


    def configure(self, recipe):
        """
        Configure the self according to recipe.
        """
        for item in recipe:
            if item[0] == 'bond':
                bond = Bond.create(item[1])

                # In order to set bond flags, we need to down it first.
                # Fortunately, configuration flags can be mapped 1:1.
                bond.state = 'down'
                for k, v in item[2].items():
                    setattr(bond, k, v)

                # Bring the bond back up.
                bond.state = 'up'

            elif item[0] == 'enslave':
                # We need to set the interface down so we can add it
                # as a slave. Then it's automatically turned on again.
                self[item[2]].state = 'down'
                self[item[1]].slave_add(item[2])

            elif item[0] == 'bridge':
                # Bridges need to be set up manually.
                Bridge.create(item[1]).state = 'up'

            elif item[0] == 'port':
                self[item[1]].port_add(item[2])

            elif item[0] == 'vlan':
                # VLANs need to be set up manually.
                VLAN.create(item[2], item[3]).state = 'up'

            elif item[0] == 'addr':
                self[item[1]].addr_add(item[2])

        return


    def unconfigure(self, recipe):
        """
        Undo all changes done by calling configure with given recipe
        """

        # TODO: We are going to fail

        for item in reversed(recipe):
            if item[0] == 'bond':
                self[item[1]].destroy()
            elif item[0] == 'enslave':
                self[item[1]].slave_del(item[2])
            elif item[0] == 'bridge':
                self[item[1]].destroy()
            elif item[0] == 'port':
                self[item[1]].port_del(item[2])
            elif item[0] == 'vlan':
                self[item[1]].destroy()
            elif item[0] == 'addr':
                self[item[1]].addr_del(item[2])
        return


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
