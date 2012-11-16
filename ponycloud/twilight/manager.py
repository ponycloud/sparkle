#!/usr/bin/python -tt

__all__ = ['Manager']

from twisted.internet import reactor, task

from ponycloud.common.util import uuidgen
from ponycloud.common.model import Model

from ponyvirt import Hypervisor

import network
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

        # TODO: After each update trigger network, storage and libvirt
        #       reconfiguration to take care of any changes.
        #       These should be written to ensure proper state in the
        #       least disruptive way.

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
        """Handler of background udev notifications."""
        if device.subsystem not in ('net', 'block'):
            return

        print 'udev event:', action, device.subsystem, device.sys_name


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
