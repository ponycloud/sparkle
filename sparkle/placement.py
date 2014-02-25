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


    def on_nic_changed(self, old, new):
        """
        Place nic on it's respective host.
        """

        if new.desired:
            host = new.desired['host']
            self.manager.bestow(host, new, new)
        elif old.desired:
            self.manager.withdraw_all(old, old)


    def on_bond_changed(self, old, new):
        """
        Place bond on it's respective host.
        """

        if new.desired:
            host = new.desired['host']
            self.manager.bestow(host, new, new)
        elif old.desired:
            self.manager.withdraw_all(old, old)


    def on_nic_role_changed(self, old, new):
        """
        Place network role for a host matched through the role's bond.
        """

        if new.desired:
            bond = self.manager.model['bond'][new.desired['bond']]
            self.manager.bestow(bond.desired['host'], new, new)
        elif old.desired:
            self.manager.withdraw_all(old, old)


    def maybe_bestow_storage_pool(self, host_id, pool_id):
        """
        Determinine whenever the given host fullfilled requirements
        (in terms of present disks) for the given storage pool.
        """

        # Get disks configured for the given storage pool.
        disks = self.manager.model['disk'].list(storage_pool=pool_id)
        disks = set([d.pkey for d in disks])

        # Get disks actually present on a given host.
        host_disks = self.manager.model['host_disk'].list(host=host_id)
        host_disks = set([hd.current['disk'] for hd in host_disks])

        # All the disks from the storage pool have to present.
        if disks.issubset(host_disks):
            storage_pool = ('storage_pool', pool_id)
            host = ('host', host_id)
            self.manager.bestow(host_id, storage_pool, host)


    def on_host_disk_changed(self, old, new):
        """
        Place configuration for actually present disks.
        """

        if new.current:
            host = new.current['host']
            disk = ('disk', new.current['disk'])
            self.manager.bestow(host, disk, new)

            # Find storage pool for this host disk.
            disk_row = self.manager.model['disk'].get(disk[1])
            pool = disk_row and disk_row.desired['storage_pool']

            # We might have enough disks now, try place the pool.
            if pool:
                self.maybe_bestow_storage_pool(host, pool)

        elif old.current:
            host = old.current['host']
            disk = ('disk', old.current['disk'])
            self.manager.withdraw(host, disk, old)

            # Find storage pool for this host disk.
            disk_row = self.manager.model['disk'].get(disk[1])
            pool = disk_row and disk_row.desired['storage_pool']

            # Withdraw the storage pool since it's definitely not complete.
            if pool:
                pool = ('storage_pool', pool)
                host = ('host', host)
                self.manager.withdraw(host, pool, host)


    def lookup_sp_hosts(self, pool):
        """
        Looks up hosts that can see any disk from the given storage pool.
        """

        hds = self.manager.model['host_disk'].list(storage_pool=pool)
        return set([hd.current['host'] for hd in hds])


    def on_storage_pool_changed(self, old, new):
        """
        Place storage pool on it's respective hosts.
        """

        # Lookup the hosts we're going to notify and check their eligibility.
        if new.desired:
            for host in self.lookup_sp_hosts(new.pkey):
                self.maybe_bestow_storage_pool(host, new.pkey)
        elif old.desired:
            for host in self.lookup_sp_hosts(old.pkey):
                self.manager.withdraw(host, old, ('host', host))


# vim:set sw=4 ts=4 et:
