#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import reactor, task
from twisted.internet.threads import deferToThread

from ponycloud.common.util import uuidgen
from ponycloud.common.model import Model
from ponycloud.twilight.network import *

from ponyvirt import Hypervisor

import pyudev


def from_thread(fn):
    """Returns function that forwards call from other thread."""
    return lambda *a, **kw: reactor.callFromThread(fn, *a, **kw)


class Manager(object):
    """
    The main application logic of Twilight.
    """

    def __init__(self, sparkle):
        """
        Stores the Sparkle connection for later use and connects to libvirt.
        """
        self.sparkle = sparkle
        self.incarnation = uuidgen()
        self.sparkle_incarnation = None
        self.outseq = 1
        self.inseq = 0

        # System configuration tools.
        self.networking = Networking()
        self.bondseq = 0
        self.brseq = 0

        print 'connecting to libvirt'
        self.virt = Hypervisor()
        self.uuid = self.virt.sysinfo['system']['uuid'].lower()

        # Our primary configuration store.  Seed with our identity.
        self.model = Model()
        self.model.load([('host', self.uuid, 'current', {'uuid': self.uuid})])

        print 'connecting to udev'
        self.udev = pyudev.Context()
        self.monitor = pyudev.Monitor.from_netlink(self.udev)
        self.observer = pyudev.MonitorObserver(self.monitor, \
                                               from_thread(self.udev_event))
        self.observer.start()


    def start(self):
        """Starts periodic tasks."""
        print 'starting manager'

        # Send empty changes every 15 seconds to make sure Sparkle
        # knows about us.  If we do not do this, we risk being fenced.
        task.LoopingCall(self.apply_changes, []).start(15.0)


    def sparkle_state_update(self, incarnation, changes, seq):
        """Handler for desired state replication from Sparkle."""

        if self.sparkle_incarnation != incarnation or self.inseq != seq:
            if seq > 0:
                print 'requesting resync with sparkle'
                self.sparkle.send({'event': 'twilight-resync',
                                   'uuid': self.uuid})
                self.sparkle_incarnation = incarnation
                self.inseq = 0
                return

        if seq == 0:
            print 'loading completely new desired state'
            new_model = Model()
            new_model.load(self.model.dump(['current']))
            self.model = new_model

        self.model.load(changes)
        self.inseq += 1


    def sparkle_resync(self):
        """Sends complete current state to Sparkle."""
        print 'sending full current state to sparkle'
        self.sparkle.send({
            'uuid': self.uuid,
            'incarnation': self.incarnation,
            'seq': 0,
            'event': 'twilight-state-update',
            'changes': self.model.dump(['current']),
        })
        self.outseq = 1


    def apply_changes(self, changes):
        """
        Applies changes to the model and forwards them to Sparkle.

        Twilights are not supposed to send desired state,
        so make sure you only update current state through here.
        """
        self.model.load(changes)
        self.sparkle.send({
            'uuid': self.uuid,
            'incarnation': self.incarnation,
            'seq': self.outseq,
            'event': 'twilight-state-update',
            'changes': changes,
        })
        self.outseq += 1


    def udev_event(self, action, device):
        """Handler for udev notifications."""

        if device.subsystem not in ('net', 'block'):
            # We are only interested in storage and network interfaces.
            return

        print 'udev event:', action, device.subsystem, device.sys_name

        if device.subsystem == 'net':
            if action == 'add':
                iface = self.networking[device.sys_name]
                if isinstance(iface, Physical):
                    # If the physical interface appeared just now,
                    # pair it up correctly with the configuration row.
                    if iface.hwaddr in self.model['nic']:
                        self.model['nic'].update_row(iface.hwaddr, 'current', {
                            'nic_name': device.sys_name,
                        })

            row = self.model['nic'].one(nic_name=device.sys_name)
            if row is not None:
                return self.nic_event(action, row)

            row = self.model['bond'].one(bond_name=device.sys_name)
            if row is not None:
                return self.bond_event(action, row)

            row = self.model['nic_role'].one(vlan_name=device.sys_name)
            if row is not None:
                return self.vlan_event(action, row)

            row = self.model['nic_role'].one(bridge_name=device.sys_name)
            if row is not None:
                return self.bridge_event(action, row)


    def create_bond(self, uuid):
        """Make sure bond with given uuid exists."""

        if self.model['bond'][uuid].get('bond_name') is not None:
            # According to current state the bond already exists.
            return False

        # We don't have interface for this row, create one.
        # No need to configure it right now, we'll get notified later.
        print 'create bond pc-bond%i' % self.bondseq
        bond = Bond.create('pc-bond%i' % self.bondseq)
        self.bondseq += 1

        # And remember it was for this row.
        self.model['bond'].update_row(uuid, 'current', {
            'bond_name': bond.name,
        })

        return True


    def configure_bond(self, row):
        """Configures an existing bond interface to match desired state."""

        # Get the configuration proxy object.
        bond = self.networking[row['bond_name']]

        # Bring the interface down in order to configure it.
        bond.state = 'down'

        # Configure the bond interface according to the desired state.
        for k, v in row.desired.items():
            if k in ('mode', 'lacp_rate', 'xmit_hash_policy'):
                if v is not None:
                    print 'setting %s.%s = %s' % (bond.name, k, v)
                    setattr(bond, k, v)

        # Bring it back up once everything is set.
        bond.state = 'up'


    def enslave_bond_interfaces(self, row):
        """Enslaves present interfaces that are to be enslaved by this bond."""

        # Get the configuration proxy object.
        bond = self.networking[row['bond_name']]

        # Add missing slaves.
        for slave in self.model['nic'].list(bond=row.pkey):
            slave_iface = slave.get('nic_name')
            if slave_iface is not None:
                if slave_iface not in bond.slaves:
                    print 'enslave %s %s' % (bond.name, slave_iface)
                    self.networking[slave_iface].state = 'down'
                    bond.slave_add(slave_iface)


    def nic_event(self, action, row):
        print 'nic event', action, row.pkey

        if action == 'add':
            if row.desired['bond'] is not None:
                # Create the bond.
                if not self.create_bond(row.desired['bond']):
                    # It was already there, do just the enslavement.
                    bond_row = self.model['bond'][row.desired['bond']]
                    self.bond_event('enslave', bond_row)

        elif action == 'remove':
            # Forget about the interface.
            self.model['nic'].update_row(row.pkey, 'current', {
                'nic_name': None,
            })


    def bond_event(self, action, row):
        print 'bond event', action, row.pkey

        # Get the network interface for configuration.
        bond = self.networking.get(row['bond_name'])

        if action == 'add':
            self.configure_bond(row)

        if action in ('add', 'enslave'):
            self.enslave_bond_interfaces(row)

        if action == 'remove':
            # Forget the bond interface.
            self.model['bond'].update_row(row.pkey, 'current', {
                'bond_name': None,
            })


    def vlan_event(self, action, row):
        print 'vlan event', action, row.pkey


    def bridge_event(self, action, row):
        print 'bridge event', action, row.pkey


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
