#!/usr/bin/python -tt
# -*- coding: utf-8 -*-


def desired_property_changed(old, new, prop):
    if old.desired and new.desired:
        return old.desired[prop] != new.desired[prop]
    return False


def current_property_changed(old, new, prop):
    if old.current and new.current:
        return old.current[prop] != new.current[prop]
    return False


def need_to_withdraw_old(old, new, prop):
    if old.desired and not new.desired:
        return True

    return desired_property_changed(old, new, prop)


def lookup_sp_hosts(model, pool_id):
    """
    Look up hosts that can see any disk from the given storage pool.
    """

    result = set()

    for disk_id in model['disk'].list_keys(storage_pool=pool_id):
        for hd in model['host_disk'].list_keys(disk=disk_id):
            result.add(hd[0])

    return result


def lookup_host_storage_pools(model, host_id):
    """
    Look up storage pools that can be (at least partially) seen from the
    specified host.
    """

    result = set()

    for hd in model['host_disk'].list_keys(host=host_id):
        disk = model['disk'].get(hd[1])
        if disk and disk.get_desired('storage_pool'):
            result.add(disk.desired['storage_pool'])

    return result


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

            # TODO: Place all iSCSI storage pools on the host.

        else:
            # Host have lost it's configuration, withdraw it.
            self.manager.withdraw(old.pkey, old)

            # TODO: Withdraw all iSCSI storage pools from the host.


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


    def on_disk_changed(self, old, new):
        if new.get_desired('storage_pool'):
            pool = new.model['storage_pool'][new.desired['storage_pool']]
            self.generic_storage_pool_placement(new.model, pool.pkey)

        if old.get_desired('storage_pool'):
            if desired_property_changed(old, new, 'storage_pool'):
                pool_id = old.desired['storage_pool']
                self.generic_storage_pool_placement(new.model, pool_id)


    def on_host_disk_changed(self, old, new):
        if new.current:
            self.manager.bestow(new.pkey[0], ('disk', new.pkey[1]))

            disk = new.model['disk'].get(new.current['disk'])
            if disk:
                pool_id = disk.desired['storage_pool']
                if pool_id:
                    self.generic_storage_pool_placement(new.model, pool_id)

        if old.current:
            self.manager.withdraw(old.pkey[0], ('disk', old.pkey[1]))

            disk = old.model['disk'].get(old.current['disk'])
            if disk:
                pool_id = disk.desired['storage_pool']
                if pool_id:
                    self.generic_storage_pool_placement(new.model, pool_id)


    def generic_storage_pool_placement(self, model, pool_id):
        """
        Place the pool on all hosts that can see at least one disk from the
        pool and withdraw it from those that have it but can't.
        """

        # Get hosts than can see a portion of the storage pool.
        sp_hosts = lookup_sp_hosts(model, pool_id)

        # Get disks forming the storage pool.
        sp_disks = model['disk'].list(storage_pool=pool_id)

        # Try to retrieve the storage pool object.
        pool = model['storage_pool'].get(pool_id)

        if pool and pool.desired:
            # Bestow the storage pool to those hosts.
            for host_id in sp_hosts:
                self.manager.bestow(host_id, pool)

                # But also bring along all the storage pool disks.
                for disk in sp_disks:
                    self.manager.bestow(host_id, disk, pool)

        # Now withdraw it from all other hosts.
        for host_id in self.manager.rows.get(('storage_pool', pool_id), []):
            pool_tuple = ('storage_pool', pool_id)

            if host_id not in sp_hosts:
                # Withdraw the storage pool.
                self.manager.withdraw(host_id, pool_tuple)

                # And once again, bring along the disks.
                for disk in sp_disks:
                    self.manager.withdraw(host_id, disk, pool_tuple)


    def on_storage_pool_changed(self, old, new):
        self.generic_storage_pool_placement(new.model, new.pkey)


# vim:set sw=4 ts=4 et:
