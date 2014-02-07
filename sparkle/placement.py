#!/usr/bin/python -tt
# -*- coding: utf-8 -*-


class Placement(object):
    """Encapsulates placement algorithms."""

    def __init__(self, manager):
        """Remember the manager."""
        self.manager = manager

    def on_row_changed(self, old, new):
        """
        Triggered on every row change.
        """

        handler = 'on_' + old.table.name + '_changed'
        if hasattr(self, handler):
            return getattr(self, handler)(old, new)
        else:
            print 'no placement routine for %s' % (old.table.name,)

    def on_host_changed(self, old, new):
        """
        By default, hosts are placed on "themselves" for "themselves".
        """

        if new.desired:
            self.manager.bestow(new.pkey, new, new)
        else:
            self.manager.withdraw(new.pkey, old, old)

    def on_host_disk_changed(self, old, new):
        """
        Place configuration for actually present disks.
        """

        if new.current:
            host = new.current['host']
            disk = ('disk', new.current['disk'])
            self.manager.bestow(host, disk, new)
        else:
            host = old.current['host']
            disk = ('disk', old.current['disk'])
            self.manager.withdraw(host, disk, old)

    def on_nic_changed(self, old, new):
        """
        Place nic on it's respective host.
        """

        if new.desired:
            host = new.desired['host']
            self.manager.bestow(host, new, new)
        elif old.desired:
            host = old.desired['host']
            self.manager.withdraw(host, old, old)

    def on_bond_changed(self, old, new):
        """
        Place bond on it's respective host.
        """

        if new.desired:
            host = new.desired['host']
            self.manager.bestow(host, new, new)
        elif old.desired:
            host = old.desired['host']
            self.manager.withdraw(host, old, old)

    def on_nic_role_changed(self, old, new):
        """
        Place network role for a host matched through the role's bond.
        """

        if new.desired:
            bond = self.manager.model['bond'][new.desired['bond']]
            host = bond.desired['host']
            self.manager.bestow(host, new, bond)
        elif old.desired:
            bond = self.manager.model['bond'][old.desired['bond']]
            host = bond.desired['host']
            self.manager.withdraw(host, old, bond)

# vim:set sw=4 ts=4 et:
