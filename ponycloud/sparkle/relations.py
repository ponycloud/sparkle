#!/usr/bin/python -tt

__all__ = ['relate']

def relate(db):
    """Tells sqlsoup about table dependencies."""
    db.tenant.relate('instance_list', db.instance)
    db.tenant.relate('user_list', db.tenant_user)
    db.tenant.relate('image_list', db.image)
    db.tenant.relate('vswitch_list', db.vswitch)
    db.tenant.relate('cluster_list', db.cluster)
    db.tenant.relate('volume_list', db.volume)
    db.tenant.relate('quota_list', db.quota)

    db.instance.relate('vdisk_list', db.vdisk)
    db.instance.relate('vnic_list', db.vnic)
    db.instance.relate('cluster_list', db.cluster_instance)

    db.vdisk.relate('volume_item', db.volume)

    db.vnic.relate('address_list', db.address)

    db.vswitch.relate('network_list', db.network)
    db.vswitch.relate('vnic_list', db.vnic)

    db.network.relate('route_list', db.route)

    db.host.relate('nic_list', db.nic)
    db.host.relate('raid_list', db.raid)
    db.host.relate('nic_failover_list', db.nic_failover)

    db.nic_failover.relate('nic_aggregation_list', db.nic_aggregation)
    db.nic_failover.relate('logical_nic_list', db.logical_nic)

    db.raid.relate('logical_volume_list', db.logical_volume)

    db.storage_pool.relate('logical_volume_list', db.logical_volume)
    db.storage_pool.relate('volume_list', db.volume)
    db.storage_pool.relate('disk_list', db.disk)

    db.volume.relate('extent_list', db.extent)
    db.volume.relate('vdisk_list', db.vdisk)

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
