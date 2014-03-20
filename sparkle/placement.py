#!/usr/bin/python -tt
# -*- coding: utf-8 -*-


class Placement(object):
    """Encapsulates placement algorithms."""

    def __init__(self, manager):
        """Remember the manager."""
        self.manager = manager


    def damage(self, row):
        """
        Map rows affected by update.

        Must yield any and all rows affected by update of the given row.
        The row itself must be included as well.
        """

        todo = [row]
        done = set()

        while len(todo):
            row = todo.pop(0)
            handler = 'damage_' + row.table.name

            if hasattr(self, handler):
                if (row.table.name, row.pkey) in done:
                    continue

                for sub in getattr(self, handler)(row):
                    todo.append(sub)

                done.add((row.table.name, row.pkey))
            else:
                print 'Placement.%s not found' % (handler,)

            yield row


    def repair(self, row):
        """
        Reconstruct placement of specified row.

        Must yield any and all host uuids the row shall be bestowed to.
        """

        # Do not bother calculating anything if the row is not configured.
        # We do not have any current-state-only placable rows.
        if not row.desired:
            return

        handler = 'repair_' + row.table.name

        if hasattr(self, handler):
            for sub in getattr(self, handler)(row):
                yield sub
        else:
            print 'Placement.%s not found' % (handler,)


    def damage_host(self, row):
        for nic in row.m.nic.list(host=row.pkey):
            yield nic

        for bond in row.m.bond.list(host=row.pkey):
            yield bond

        for host_disk in row.m.host_disk.list(host=row.pkey):
            yield host_disk


    def repair_host(self, row):
        yield row.pkey


    def damage_bond(self, row):
        for nic_role in row.m.nic_role.list(bond=row.pkey):
            yield nic_role


    def repair_bond(self, row):
        if row.m.host[row.d.host].d.state != 'evacuated':
            yield row.d.host


    def repair_nic(self, row):
        if row.m.host[row.d.host].d.state != 'evacuated':
            yield row.d.host


    def repair_nic_role(self, row):
        bond = row.m.bond[row.d.bond]
        if bond.m.host[bond.d.host].d.state != 'evacuated':
            yield bond.d.host


    def damage_host_disk(self, row):
        for disk in row.m.disk.list(id=row.c.disk):
            yield disk


    def damage_disk(self, row):
        if row.d.storage_pool:
            yield row.m.storage_pool[row.d.storage_pool]


    def repair_disk(self, row):
        # Place disk for every host_disk.
        for host_disk in row.m.host_disk.list(disk=row.pkey):
            for host in row.m.host.list(uuid=host_disk.c.host):
                if host.d.state != 'evacuated':
                    yield host.pkey

        # Also place the disk to hosts that can see at least part of
        # disk's storage pool via *some* host_disk.
        for disk in row.m.disk.list(storage_pool=row.d.storage_pool):
            for host_disk in row.m.host_disk.list(disk=row.pkey):
                for host in row.m.host.list(uuid=host_disk.c.host):
                    if host.d.state != 'evacuated':
                        yield host.pkey


    def damage_storage_pool(self, row):
        for disk in row.m.disk.list(storage_pool=row.pkey):
            yield disk


    def repair_storage_pool(self, row):
        # Place storage pool on all hosts that can see at least a part of it.
        for disk in row.m.disk.list(storage_pool=row.pkey):
            for host_disk in row.m.host_disk.list(disk=disk.pkey):
                for host in row.m.host.list(uuid=host_disk.c.host):
                    if host.d.state != 'evacuated':
                        yield host.pkey


# vim:set sw=4 ts=4 et:
