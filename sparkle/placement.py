#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

from random import randint


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


    def select_host_for(self, row, candidates=None):
        """
        Yield a host uuid for selected row.

        Candidates can be a set used to restrict the selection to a
        particular group of hosts.  The host may not be evacuated and
        must be present (have current state).

        If a host the row is currently placed on satisfies the constraints,
        it is yielded to prevent random resource relocations.

        No hosts are yielded if none satisfy the constraints.
        """

        # Find hosts that are running and should continue doing so.
        viable = set(row.m.host.list_keys(state='present', status='present'))

        if candidates:
            # Apply further restriction from the caller.
            viable.difference_update(candidates)

        # Find out about previous placement.
        old_hosts = self.manager.rows.get((row.table.name, row.pkey), set())

        # In the ideal case, we would find our target host among the hosts
        # the row is currently placed on.
        best = old_hosts.intersection(viable)

        if best:
            yield next(iter(best))
        elif viable:
            yield list(viable)[randint(1, len(viable)) - 1]


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
                if host.d.state == 'present':
                    yield host.pkey

        # Also place the disk to hosts that can see at least part of
        # disk's storage pool via *some* host_disk.
        for disk in row.m.disk.list(storage_pool=row.d.storage_pool):
            for host_disk in row.m.host_disk.list(disk=row.pkey):
                for host in row.m.host.list(uuid=host_disk.c.host):
                    if host.d.state == 'present':
                        yield host.pkey


    def damage_storage_pool(self, row):
        for disk in row.m.disk.list(storage_pool=row.pkey):
            yield disk


    def repair_storage_pool(self, row):
        # Place storage pool on all hosts that can see at least a part of it.
        for disk in row.m.disk.list(storage_pool=row.pkey):
            for host_disk in row.m.host_disk.list(disk=disk.pkey):
                for host in row.m.host.list(uuid=host_disk.c.host):
                    if host.d.state == 'present':
                        yield host.pkey


    def damage_volume(self, row):
        for extent in row.m.extent.list(volume=row.pkey):
            yield extent

        if row.d.image:
            yield row.m.image[row.d.image]


    def repair_extent(self, row):
        if row.d.volume:
            # Some extents represent a free space and thus have no volume.
            for host in self.repair_volume(row.m.volume[row.d.volume]):
                yield host


    def repair_volume(self, row, for_images=set()):
        if row.d.state == 'deleted':
            candidates = set()

            for host_sp in row.m.host_storage_pool.list(storage_pool=row.d.storage_pool):
                if host_sp.c.status != 'ready':
                    continue

                host = row.m.host.get(host_sp.c.host)
                if not host or host.d.state != 'present' or \
                               host.c.status != 'present':
                    continue

                candidates.add(host.pkey)

            cp = self.manager.rows.get((row.table.name, row.pkey), set())
            best = cp.intersection(candidates)

            if best:
                return self.select_host_for(row, best)

            if candidates:
                return self.select_host_for(row, candidates)

        elif row.d.base_image:
            image = row.m.image[row.d.base_image]

            sps = set()
            for sv in row.m.volume.list(image=image.pkey):
                if not sv.d.base_image and sv.d.state != 'deleted':
                    sps.add(sv.d.storage_pool)

            # Candidates that can see some of the backing volumes of the
            # image the target volume is to be intialized from.
            source_hosts = set()

            for sp in sps:
                for host_sp in row.m.host_storage_pool.list(storage_pool=sp):
                    if host_sp.c.status != 'ready':
                        continue

                    host = row.m.host.get(host_sp.c.host)
                    if not host or host.d.state != 'present' or \
                                   host.c.status != 'present':
                        continue

                    source_hosts.add(host.pkey)

            # Candidates that can actually hold the target volume.
            dest_hosts = set()

            for host_sp in row.m.host_storage_pool.list(storage_pool=row.d.storage_pool):
                if host_sp.c.status != 'ready':
                    continue

                host = row.m.host.get(host_sp.c.host)
                if not host or host.d.state != 'present' or \
                               host.c.status != 'present':
                    continue

                dest_hosts.add(host.pkey)

            cp = self.manager.rows.get((row.table.name, row.pkey), set())
            candidates = source_hosts.intersection(dest_hosts)
            best = cp.intersection(candidates)

            if best:
                # Prefer not to change placement.
                return self.select_host_for(row, best)

            if candidates:
                # If we need to pick a new host, pick one with the source
                # storage pool so that we can make a disk-to-disk copy.
                return self.select_host_for(row, candidates)

            if image.d.source_uri:
                # If all fails but the image has an URI, place anywhere so
                # that the volume can be initialized via HTTP or something...
                return self.select_host_for(row, dest_hosts)

        elif row.d.image:
            # Place volume after the image it is backing.
            return self.repair_image(row.m.image[row.d.image], for_images)

        # All repair methods must produce iterators.
        return iter(())


    def damage_image(self, row):
        for volume in row.m.volume.list(base_image=row.pkey):
            yield volume


    def repair_image(self, row, for_images=set()):
        if row.pkey in for_images:
            return

        for_images = for_images.union((row.pkey,))

        for volume in row.m.volume.list(base_image=row.pkey):
            for host in self.repair_volume(volume, for_images):
                yield host


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
