#!/usr/bin/python -tt

__all__ = []

from netpony import *

network = Networking()

def create_recipe(configuration):
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
    bonds = {}
    vlans = {}
    nics  = {network[x].hwaddr: network[x].name for x in list(network) if type(network[x]) is Interface}

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
            bond_flags = ('bond-flags', name, bond['mode'])
        elif bond['mode'] == '802.3ad':
            bond_flags = ('bond-flags', name, bond['mode'], bond['lacp_rate'], bond['xmit_hash_policy'])

        # Produce the bond itself.
        out.append(('bond', name))
        out.append(bond_flags)
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
        brseq += 1
        out.append(('bridge', name))

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

def configure(recipe):
    """
    Configure the network according to recipe.
    """
    for item in recipe:
        if item[0] == 'bond':
            Bond.create(item[1])
        if item[0] == 'bond-flags':
            if item[2] == 'active-backup':
                network[item[1]].mode = item[2]
            elif item[2] == '802.3ad':
                network[item[1]].mode = item[2]
                network[item[1]].lacp_rate = item[3]
                network[item[1]].xmit_hash_policy = item[4]
        elif item[0] == 'enslave':
            #We need to set the interface down so we can add it
            #as a slave. Then it's automatically turned on again.
            network[item[2]].state = 'down'
            network[item[1]].slave_add(item[2])
        elif item[0] == 'bridge':
            #Bridges need to be set up manually.
            Bridge.create(item[1]).state = 'up'
        elif item[0] == 'port':
            network[item[1]].port_add(item[2])
        elif item[0] == 'vlan':
            #VLANs need to be set up manually.
            VLAN.create(item[2], item[3]).state = 'up'
        elif item[0] == 'addr':
            network[item[1]].addr_add(item[2])
    return

#TODO We need to be tolerant to faults so we can do as much as possible.
def unconfigure(recipe):
    """
    Undo all changes done by calling configure with given recipe
    """
    for item in reversed(recipe):
        if item[0] == 'bond':
            network[item[1]].destroy()
        elif item[0] == 'enslave':
            network[item[1]].slave_del(item[2])
        elif item[0] == 'bridge':
            network[item[1]].destroy()
        elif item[0] == 'port':
            network[item[1]].port_del(item[2])
        elif item[0] == 'vlan':
            network[item[1]].destroy()
        elif item[0] == 'addr':
            network[item[1]].addr_del(item[2])
    return

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
