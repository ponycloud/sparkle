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


    def maybe_bestow_storage_pool(self, host_id, pool_id):
        """
        Determinine whenever the given host fullfilled requirements
        (in terms of present disks) for the given storage pool.
        """

        # Get disks configured for the given storage pool.
        disks = self.manager.model['disk'].list(storage_pool=pool_uuid)
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

            # Check for possible storage pool placement.
            pool_id = self.manager.model['disk'][disk[1]].desired['storage_pool']
            self.maybe_bestow_storage_pool(host, pool_id)
        else:
            host = old.current['host']
            disk = ('disk', old.current['disk'])
            self.manager.withdraw(host, disk, old)

            # Withdraw the disk on a related host.
            pool_id = self.manager.model['disk'][disk[1]].desired['storage_pool']
            # Let's withdraw the storage pool since it's not complete.
            self.manager.withdraw(host, ('storage_pool', pool_id), ('host', host))


    def lookup_sp_hosts(self, pool_id):
        """
        Looks up hosts that can see any disk from the given storage pool.
        """
        host_disks = self.manager.model['host_disk'].list(storage_pool=pool_id)
        hosts = [hd.current['host'] for hd in host_disks]
        return set(hosts)


    def on_storage_pool_changed(self, old, new):
        """
        Place storage pool on it's respective hosts.
        """

        # Lookup the hosts we're going to notify and check their eligibility.
        if new.desired:
            hosts = self.lookup_sp_hosts(new.pkey)
            for host in hosts:
                self.maybe_bestow_storage_pool(host, new.pkey)
        else:
            hosts = self.lookup_sp_hosts(old.pkey)
            for host in hosts:
                self.manager.withdraw(host, old, ('host', host))


# vim:set sw=4 ts=4 et:
