#!/usr/bin/python -tt

from twisted.internet import reactor
from ponycloud.twilight.network import *

class NetworkManager(object):
    """Manager Mixin that takes care of network configuration."""

    def __init__(self):
        # Network configuration object.
        self.networking = Networking()

        # Sequences for bond and bridge naming.
        self.bondseq = 0
        self.brseq = 0

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


    def create_nic(self, row):
        """
        NIC has been configured.

        The NIC may now belong to a bond, which is why we registed an
        event handler that waits for both the NIC and bond to appear
        and then enslaves the NIC to the bond.

        The handler it installs is persistent and will remain active
        until the NIC have been removed.
        """

        if row.get_desired('bond') is None:
            # Do nothing if the NIC has no bond.
            return

        @self.handle_events([('add', 'bond', 'by-uuid',   row.desired['bond']),
                             ('add', 'nic',  'by-hwaddr', row.pkey)])
        def add_nic_to_bond(bond, nic):
            # Abort if the NIC should no longer be plugged into this bond.
            if nic.get_desired('bond') != bond.pkey:
                return

            # Enslave the NIC.
            nic_name = nic.current['nic_name']
            bond_name = bond.current['bond_name']

            if nic_name not in self.networking[bond_name].slaves:
                self.networking[nic_name].state = 'down'
                self.networking[bond_name].slave_add(nic_name)

                # Store the bond in the current state to let user know.
                self.apply_change('nic', row.pkey, 'current', {
                    'bond': row.desired['bond'],
                })

        # Poke network interfaces to trigger the event.
        self.poke_network_interface(row)
        self.poke_network_interface(self.model['bond'][row.desired['bond']])


    def update_nic(self, row):
        """
        NIC configuration have been changed.

        Make sure it is still connected to the correct bond and if
        it is not, connect it to the correct one.
        """

        if row.get_current('nic_name') is None:
            # Do not bother if the interface does not yet exist.
            return

        if row.get_desired('bond') != row.get_current('bond'):
            # Act as if the NIC has been deleted to remove it from the bond.
            # It also cancels the nic/bond event handler.
            self.delete_nic(row)

        # Enslave NIC to the correct bond and everything...
        self.create_nic(row)


    def delete_nic(self, row):
        """
        NIC have been deconfigured.

        We need to remove it from any bond it's a slave of and cancel
        the event handler from `create_nic()`.
        """

        # Remove the NIC from the bond, if required.
        if row.get_current('bond') is not None:
            bond = self.model['bond'][row.current['bond']]
            if bond.get_current('bond_name') is not None:
                bond = self.networking[bond.current['bond_name']]
                if row.current['nic_name'] in bond.slaves:
                    bond.slave_del(row.current['nic_name'])
                self.apply_change('nic', row.pkey, 'current', {
                    'bond': None
                })

        # Cancel event handlers.
        self.cancel_event(('add', 'nic', 'by-hwaddr', row.pkey))


    def create_bond(self, row):
        """
        A bond is to be created.

        We need to generate a name and store it in the current state of
        the bond. Then we tell the system to actually create the bond.

        Since the bond is not created immediately, we need to reflect that
        in the current state.

        The bond creation is two-phase. First we issue a request to create
        it and then, once udev says it's here, a callback configures it.
        If the bond is deconfigured in between these two steps, the
        configuration callback just destroys it.
        """

        # Verify that the event is still valid and the bond should exist.
        if row.desired is None:
            return

        # Tell system to create the bond.
        bond = Bond.create('pc-bond%i' % self.bondseq)
        self.bondseq += 1

        # Tell system to create bridge for this bond.
        bridge = Bridge.create('pc-br%i' % self.brseq)
        self.brseq += 1

        # Write down it's name and that it's not ready yet.
        self.apply_change('bond', row.pkey, 'current', {
            'bond_name': bond.name,
            'bridge_name': bridge.name,
            'state': 'creating',
        })

        # Wait 'till it appears so that we can configure it properly.
        @self.handle_events([('add', 'bond', 'by-uuid', row.pkey),
                             ('add', 'bridge', 'by-uuid', row.pkey)],
                            once=True)
        def bond_ready(row, row2):
            # Plug the bond into the bridge and set it up.
            bridge.port_add(bond.name)
            bridge.state = 'up'

            # Now we are ready, write it down.
            self.apply_change('bond', row.pkey, 'current', {
                'state': 'present',
            })

            if row.desired is not None:
                # The update handler will set things up for us.
                self.update_bond(row)
            else:
                # If the bond have been deconfigured,
                # delete handler will clean things up for us.
                self.delete_bond(row)


    def update_bond(self, row):
        """
        Bond configuration have been changed.

        If the bond is already present, we need to bring it down,
        set it's options and bring it back up. No event is fired.
        """

        if row.desired is None or row.get_current('state') is None:
            # Do nothing if the bond configuration have since disappeared or
            # the bond creation have not yet happened.
            return

        # Get the configuration proxy.
        bond = self.networking[row.current['bond_name']]

        # Remove slave interfaces for the configuration to work.
        slaves = bond.slaves
        for slave in slaves:
            bond.slave_del(slave)

        # Linux needs the bond down to change it's options.
        bond.state = 'down'

        # Set the configuration flags.
        for k in ('mode', 'xmit_hash_policy', 'lacp_mode'):
            if row.get_desired(k) is not None:
                setattr(bond, k, row.desired[k])

        # Bring the bond back up.
        bond.state = 'up'

        # Re-enslave the interfaces.
        for slave in slaves:
            bond.slave_add(slave)


    def delete_bond(self, row):
        """
        A bond is to be destroyed.

        If the bond is in the 'present' state, it is destroyed.
        The bond in the process of being created is destroyed by it's
        configure callback. See `create_bond()` for more info.
        """

        if row.get_current('state') == 'present':
            if row.get_current('bridge_name') is not None:
                bridge = self.networking[row.current['bridge_name']]
                for port in bridge.ports:
                    bridge.port_del(port)
                bridge.state = 'down'
                bridge.destroy()

            if row.get_current('bond_name') is not None:
                bond = self.networking[row.current['bond_name']]
                bond.state = 'down'
                bond.destroy()


    def create_nic_role(self, row):
        pass


    def update_nic_role(self, row):
        pass


    def delete_nic_role(self, row):
        pass


    def add_net(self, dev):
        """
        New network device have appeared.

        If the network device is a Physical NIC, we need to pair it's
        device name with the hwaddr used as the primary key in data model.
        That means locating/adding current state with the hardware addess
        mapped to nic name.

        If any network interface already has the mapping of it's primary
        key to the the device name done, also raises 'by-<pkey>' event for
        that interface. See `create_nic()`.
        """

        # Get the network device configuration proxy.
        iface = self.networking[dev.sys_name]

        # Pair up physical interfaces in current state and note that
        # it is not yet member of any bond.
        if isinstance(iface, Physical):
            self.apply_change('nic', iface.hwaddr, 'current', {
                'nic_name': dev.sys_name,
                'bond': None,
            })

            row = self.model['nic'].get(iface.hwaddr)
            if row is not None:
                self.raise_event(('add', 'nic', 'by-hwaddr', row.pkey), row)

        elif isinstance(iface, Bond):
            rows = self.model['bond'].list(bond_name=dev.sys_name)
            if len(rows) > 0:
                row = rows.pop()
                self.raise_event(('add', 'bond', 'by-uuid', row.pkey), row)

        elif isinstance(iface, VLAN):
            rows = self.model['nic_role'].list(vlan_name=dev.sys_name)
            if len(rows) > 0:
                row = rows.pop()
                self.raise_event(('add', 'vlan', 'by-uuid', row.pkey), row)

        elif isinstance(iface, Bridge):
            rows = self.model['nic_role'].list(bridge_name=dev.sys_name) \
                 + self.model['bond'].list(bridge_name=dev.sys_name)
            if len(rows) > 0:
                row = rows.pop()
                self.raise_event(('add', 'bridge', 'by-uuid', row.pkey), row)


    def change_net(self, dev):
        pass


    def remove_net(self, dev):
        """
        A network interface have disappeared.

        All interface types need to have their current state removed if
        the interface disappears. If the interface have been a bond,
        all it's NICs need to have the bond removed from their current state.
        """

        row = None

        rows = self.model['nic'].list(nic_name=dev.sys_name)
        if len(rows) > 0:
            row = rows.pop()
            self.apply_change('nic', row.pkey, 'current', None)
            return

        rows = self.model['bond'].list(bond_name=dev.sys_name)
        if len(rows) > 0:
            row = rows.pop()
            self.apply_change('bond', row.pkey, 'current', None)

            for nic in self.model['nic'].list(bond=row.pkey):
                if nic.get_desired('bond') == row.pkey:
                    self.apply_change('nic', nic.pkey, 'current', {
                        'bond': None,
                    })

            return


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
                        iface = self.networking[row.current[key]]
                        iface.state = 'down'
                        iface.destroy()


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
