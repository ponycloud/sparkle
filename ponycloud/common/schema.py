#!/usr/bin/python -tt

__all__ = ['SCHEMA']

SCHEMA = {
    'address': {
        'pkey': ['uuid'],
        'parents': [['network', 'network'],
                    ['vnic', 'vnic']],
        'owner': ['vnic', 'vnic'],
    },
    'bond': {
        'pkey': ['uuid'],
        'parents': [['host', 'host']],
        'owner': None,
    },
    'cluster': {
        'pkey': ['uuid'],
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
    'cluster_instance': {
        'pkey': ['uuid'],
        'parents': [['cluster', 'cluster'],
                    ['instance', 'instance']],
        'owner': ['cluster', 'cluster'],
    },
    'cpu_profile': {
        'pkey': ['uuid'],
        'parents': [],
        'owner': None,
        'public': True,
    },
    'config': {
        'pkey': ['key'],
        'parents': [],
        'owner': None,
    },
    'disk': {
        'pkey': ['id'],
        'parents': [['storage_pool', 'storage_pool']],
        'owner': None,
    },
    'extent': {
        'pkey': ['uuid'],
        'parents': [['storage_pool', 'storage_pool'],
                    ['volume', 'volume']],
        'owner': None,
    },
    'event': {
        'pkey': ['hash'],
        'parents': [['host', 'host'],
                    ['instance', 'instance']],
        'owner': ['instance', 'instance'],
    },
    'host': {
        'pkey': ['uuid'],
        'parents': [],
        'owner': None,
    },
    'instance': {
        'pkey': ['uuid'],
        # ['local column', 'remote table']
        'parents': [['cpu_profile', 'cpu_profile'],
                    ['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
    'image': {
        'pkey': ['uuid'],
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
        'public': True,
    },
    'member': {
        'pkey': ['uuid'],
        'parents': [['tenant', 'tenant'],
                    ['user', 'user']],
        'owner': ['tenant', 'tenant'],
    },
    'network': {
        'pkey': ['uuid'],
        'parents': [['switch', 'switch']],
        'owner': ['switch', 'switch'],
        'public': True
    },
    'image': {
        'pkey': ['uuid'],
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
        'public': True
    },
    'nic': {
        'pkey': ['hwaddr'],
        'parents': [['host', 'host'],
                    ['bond', 'bond']],
        'owner': None,
    },
    'nic_role': {
        'pkey': ['uuid'],
        'parents': [['bond', 'bond']],
        'owner': None,
    },
    'quota': {
        'pkey': ['uuid'],
        'parents': [['tenant', 'tenant']],
        'owner': None,
    },
    'route': {
        'pkey': ['uuid'],
        'parents': [['network', 'network']],
        'owner': ['network', 'network'],
    },
    'storage_pool': {
        'pkey': ['uuid'],
        'parents': [],
        'owner': None,
        'public': True,
    },
    'switch': {
        'pkey': ['uuid'],
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
    'tenant': {
        'pkey': ['uuid'],
        'parents': [],
        'owner': None,
    },
    'user': {
        'pkey': ['email'],
        'parents': [],
        'owner': None,
    },
    'vdisk': {
        'pkey': ['uuid'],
        'parents': [['instance', 'instance'],
                    ['volume', 'volume'],
                    ['storage_pool', 'storage_pool']],
        'owner': ['instance', 'instance'],
    },
    'vnic': {
        'pkey': ['uuid'],
        'parents': [['instance', 'instance'],
                    ['switch', 'switch']],
        'owner': ['instance', 'instance'],
    },
    'volume': {
        'pkey': ['uuid'],
        'parents': [['storage_pool', 'storage_pool'],
                    ['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
}

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
