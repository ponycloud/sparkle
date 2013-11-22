#!/usr/bin/python -tt

__all__ = ['schema']


class Schema(dict):
    def __init__(self, *args, **kwargs):
        self.schema = SCHEMA
        for item in self.schema:
            for parent in self.schema[item]['parents']:
                local_key, table_name = parent
                if 'children' not in self.schema[table_name]:
                    self.schema[table_name]['children'] = []
                self.schema[table_name]['children'].append([local_key, item])
            if 'children' not in self.schema[item]:
                self.schema[item]['children'] = []

    def __getitem__(self, key):
        return self.schema[key]

    def __setitem__(self, key, value):
        raise Exception('Schema is immutable')

    def __delitem__(self, key):
        raise Exception('Schema is immutable')

    def __iter__(self):
        return iter(self.schema)

    def __len__(self):
        return len(self.schema)

    def __repr__(self):
        return str(self.schema)

    def get_fkey(self, table_name, remote_table):
        for parent in self.schema[table_name]['parents']:
            local_key, parent_table = parent
            if parent_table == remote_table:
                return local_key
        return None

SCHEMA = {
    'address': {
        'pkey': 'uuid',
        # [local_key, remote_table]
        'parents': [['network', 'network'],
                    ['vnic', 'vnic']],
        'owner': ['vnic', 'vnic'],
    },
    'bond': {
        'pkey': 'uuid',
        'parents': [['host', 'host']],
        'owner': None,
    },
    'cluster': {
        'pkey': 'uuid',
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
    'cluster_instance': {
        'pkey': 'uuid',
        'parents': [['cluster', 'cluster'],
                    ['instance', 'instance']],
        'owner': ['cluster', 'cluster'],
    },
    'cpu_profile': {
        'pkey': 'uuid',
        'parents': [],
        'owner': None,
        'public': True,
    },
    'config': {
        'pkey': 'key',
        'parents': [],
        'owner': None,
    },
    'disk': {
        'pkey': 'id',
        'parents': [['storage_pool', 'storage_pool'],
                    ['disk', 'disk']],
        'owner': None,
    },
    'extent': {
        'pkey': 'uuid',
        'parents': [['storage_pool', 'storage_pool'],
                    ['volume', 'volume']],
        'owner': None,
    },
    'event': {
        'pkey': 'hash',
        'parents': [['host', 'host'],
                    ['instance', 'instance']],
        'owner': ['instance', 'instance'],
    },
    'host': {
        'pkey': 'uuid',
        'parents': [],
        'owner': None,
    },
    'instance': {
        'pkey': 'uuid',
        'parents': [['cpu_profile', 'cpu_profile'],
                    ['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
    'image': {
        'pkey': 'uuid',
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
        'public': True,
    },
    'member': {
        'pkey': 'uuid',
        'parents': [['tenant', 'tenant'],
                    ['user', 'user']],
        'owner': ['tenant', 'tenant'],
    },
    'network': {
        'pkey': 'uuid',
        'parents': [['switch', 'switch']],
        'owner': ['switch', 'switch'],
        'public': True
    },
    'image': {
        'pkey': 'uuid',
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
        'public': True
    },
    'nic': {
        'pkey': 'hwaddr',
        'parents': [['host', 'host'],
                    ['bond', 'bond']],
        'owner': None,
    },
    'nic_role': {
        'pkey': 'uuid',
        'parents': [['bond', 'bond']],
        'owner': None,
    },
    'quota': {
        'pkey': 'uuid',
        'parents': [['tenant', 'tenant']],
        'owner': None,
    },
    'route': {
        'pkey': 'uuid',
        'parents': [['network', 'network']],
        'owner': ['network', 'network'],
    },
    'storage_pool': {
        'pkey': 'uuid',
        'parents': [],
        'owner': None,
        'public': True,
    },
    'switch': {
        'pkey': 'uuid',
        'parents': [['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
    'tenant': {
        'pkey': 'uuid',
        'parents': [],
        'owner': None,
    },
    'user': {
        'pkey': 'email',
        'parents': [],
        'owner': None,
    },
    'vdisk': {
        'pkey': 'uuid',
        'parents': [['instance', 'instance'],
                    ['volume', 'volume'],
                    ['storage_pool', 'storage_pool']],
        'owner': ['instance', 'instance'],
    },
    'vnic': {
        'pkey': 'uuid',
        'parents': [['instance', 'instance'],
                    ['switch', 'switch']],
        'owner': ['instance', 'instance'],
    },
    'volume': {
        'pkey': 'uuid',
        'parents': [['storage_pool', 'storage_pool'],
                    ['tenant', 'tenant']],
        'owner': ['tenant', 'tenant'],
    },
}

schema = Schema()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
