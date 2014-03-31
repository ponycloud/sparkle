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
        for host_disk in row.m.host_disk.list(host=row.pkey):
            yield host_disk

        # Abuse information about current placement of the volumes.
        if row.pkey in self.manager.hosts:
            host = self.manager.hosts[row.pkey]
            volumes = host.desired.get('volume', set())
            for volume in volumes:
                yield row.m.volume[volume]


    def repair_host(self, row):
        yield row.pkey


    def damage_bond(self, row):
        for nic_role in row.m.nic_role.list(bond=row.pkey):
            yield nic_role


    def repair_bond(self, row):
        yield row.d.host


    def repair_nic(self, row):
        yield row.d.host


    def repair_nic_role(self, row):
        yield row.m.bond[row.d.bond].d.host


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


    def damage_volume(self, row):
        for extent in row.m.extent.list(volume=row.pkey):
            yield extent

        if row.d.image:
            yield row.m.image[row.d.image]


    def repair_extent(self, row):
        volume = row.m.volume[row.d.volume]
        return self.repair_volume(volume)


    def repair_volume(self, row):
        if row.d.state == 'deleted':
            candidates = set()

            for host_sp in row.m.host_storage_pool.list(storage_pool=row.d.storage_pool):
                if host_sp.c.status != 'ready':
                    continue

                host = row.m.host.get(host_sp.c.host)
                if not host or host.d.state == 'evacuated':
                    continue

                candidates.add(host.pkey)

            cp = self.manager.rows.get((row.table.name, row.pkey), set())
            best = cp.intersection(candidates)

            if best:
                yield next(iter(best))
            elif candidates:
                yield next(iter(candidates))

        elif row.d.base_image:
            sps = set()
            image = row.m.image[row.d.base_image]

            if image.d.source_uri:
                # We can place anywhere
                continue

            for sv in row.m.volume.list(image=image.pkey):
                if not sv.d.base_image and sv.d.state != 'deleted':
                    sps.add(sv.d.storage_pool)

            candidates = set()

            # Candidates that can see backing volumes of an image
            # the volume should be intialized from.
            for sp in sps:
                for host_sp in row.m.host_storage_pool.list(storage_pool=sp):
                    if host_sp.c.status != 'ready':
                        continue

                    host = row.m.host.get(host_sp.c.host)
                    if not host or host.d.state == 'evacuated':
                        continue

                    candidates.add(host.pkey)

            # Candidates that are based on a storagepool the volume belongs to.
            for host_sp in row.m.host_storage_pool.list(storage_pool=row.d.storage_pool):
                 if host_sp.c.status != 'ready':
                    continue

                host = row.m.host.get(host_sp.c.host)
                if not host or host.d.state == 'evacuated':
                    continue

                candidates.add(host.pkey)

            cp = self.manager.rows.get((row.table.name, row.pkey), set())
            best = cp.intersection(candidates)

            if best:
                yield next(iter(best))
            elif candidates:
                yield next(iter(candidates))


    def damage_image(self, row):
        for volume in row.m.volume.list(base_image=row.pkey):
            yield volume


    def damage_host_storage_pool(self, row):
        if row.c.host in self.manager.hosts:
            host = self.manager.hosts[row.c.host]
            h_volumes = host.desired.get('volume', set())
            for volume in h_volumes:
                yield row.m.volume[volume]

        # This could have been written in a simple way,
        # but we expect to have a whole lot of volumes and it would
        # not be very wise to iterate over them every time a host
        # tells us about a minor change of a storage pool.
        allv = row.m.volume.list_keys(storage_pool=row.c.storage_pool)
        spdv = row.m.volume.list_keys(storage_pool=row.c.storage_pool,
                                      state='deleted')
        nonbiv = row.m.volume.list_keys(storage_pool=row.c.storage_pool,
                                        base_image=None)

        # Damage volumes that are currently not placed
        # and are to be deleted or initialized.
        sp_volumes = allv.difference(nonbiv).union(spdv)

        for volume in sp_volumes:
            if not self.manager.rows.get(('volume', volume), set()):
                yield row.m.volume[volume]


# vim:set sw=4 ts=4 et:
