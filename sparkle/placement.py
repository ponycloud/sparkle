#!/usr/bin/python -tt
# -*- coding: utf-8 -*-


def desired_property_changed(old, new, prop):
    if old.desired and new.desired:
        return old.desired[prop] != new.desired[prop]
    return False


def need_to_withdraw_old(old, new, prop):
    if old.desired and not new.desired:
        return True

    return desired_property_changed(old, new, prop)


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
        if new.desired:
            # Host is configured, place it on itself.
            self.manager.bestow(new.pkey, new)

        else:
            # Host have lost it's configuration, withdraw it.
            self.manager.withdraw(old.pkey, old)


    def generic_host_child_handler(self, old, new):
        if new.desired:
            self.manager.bestow(new.desired['host'], new)

        if need_to_withdraw_old(old, new, 'host'):
            self.manager.withdraw(old.desired['host'], old)


    def on_nic_changed(self, old, new):
        return self.generic_host_child_handler(old, new)


    def on_bond_changed(self, old, new):
        return self.generic_host_child_handler(old, new)


    def on_nic_role_changed(self, old, new):
        if new.desired:
            bond = new.model['bond'][new.desired['bond']]
            self.manager.bestow(bond.desired['host'], new)

        if need_to_withdraw_old(old, new, 'bond'):
            bond = old.model['bond'][old.desired['bond']]
            self.manager.withdraw(bond.desired['host'], old)


    def on_host_disk_changed(self, old, new):
        if new.current:
            host_id = new.current['host']
            self.manager.bestow(host_id, ('disk', new.current['disk']), new)
            disk = new.model['disk'].get(new.current['disk'])

            if disk:
                pool_id = disk.desired['storage_pool']
                if pool_id:
                    pool = new.model['storage_pool'][pool_id]
                    self.maybe_bestow_storage_pool(host_id, pool)

        elif old.current:
            host_id = old.current['host']
            self.manager.withdraw(host_id, ('disk', old.current['disk']), old)
            disk = old.model['disk'].get(old.current['disk'])

            if disk:

                pool_id = disk.desired['storage_pool']
                if pool_id:
                    self.manager.withdraw(host_id, ('storage_pool', pool_id))


    def maybe_bestow_storage_pool(self, host_id, pool):
        """
        Determinine whether the given host can access all required disks
        and if it can, bestow the storage pool.
        """

        # Get disks configured for the storage pool in question.
        disks = pool.model['disk'].list_keys(storage_pool=pool.pkey)

        # Get disks actually present on the specified host.
        host_disks = pool.model['host_disk'].list_keys(host=host_id)
        host_disks = set([k[1] for k in host_disks])

        if disks.issubset(host_disks):
            # All the disks from the storage pool are present...
            self.manager.bestow(host_id, pool)


    def lookup_sp_hosts(self, pool):
        """
        Look up hosts that can see any disk from the given storage pool.
        """

        hds = pool.model['host_disk'].list(storage_pool=pool)
        return set([hd.current['host'] for hd in hds])


    def on_storage_pool_changed(self, old, new):
        if new.desired:
            hds = new.model['host_disk'].list_keys(storage_pool=new.pkey)

            for host_id in set([hd[0] for hd in hds]):
                self.maybe_bestow_storage_pool(host_id, new)

        elif old.desired:
            self.manager.withdraw_all(old)


# vim:set sw=4 ts=4 et:
