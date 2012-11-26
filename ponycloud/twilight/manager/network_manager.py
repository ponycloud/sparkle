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
            if row.get(key) is not None:
                if row[key] in self.networking:
                    dev = list(self.udev.list_devices(subsystem='net',
                                                      INTERFACE=row[key]))[0]
                    self.raise_event(('add', 'net', row.current[key]), dev)


    def create_nic(self, row):
        pass


    def update_nic(self, row):
        pass


    def delete_nic(self, row):
        pass


    def create_bond(self, row):
        pass


    def update_bond(self, row):
        pass


    def delete_bond(self, row):
        pass


    def create_nic_role(self, row):
        pass


    def update_nic_role(self, row):
        pass


    def delete_nic_role(self, row):
        pass


    def add_net(self, dev):
        pass


    def change_net(self, dev):
        pass


    def remove_net(self, dev):
        pass


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
