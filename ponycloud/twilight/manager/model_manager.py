#!/usr/bin/python -tt

from twisted.internet import task

from ponycloud.common.util import uuidgen
from ponycloud.common.model import Model

class ModelManager(object):
    """Manager mixin that takes care of model replication."""

    def __init__(self, sparkle):
        # Thingies to help us stay in touch with Sparkle.
        self.sparkle = sparkle
        self.incarnation = uuidgen()
        self.sparkle_incarnation = None
        self.outseq = 1
        self.inseq = 0

        # Our primary configuration store. Seed with our identity.
        # Identity will be sent with initial resync.
        self.model = Model()
        self.model.load([('host', self.uuid, 'current', {'uuid': self.uuid})])

        # Start watching the model for changes.
        self.watch_model()


    def start_model_tasks(self):
        """Starts model-related periodic tasks."""

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
            self.watch_model()

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


    def apply_change(self, table, pkey, state, part):
        """Applies just one change."""
        return self.apply_changes([(table, pkey, state, part)])


    def watch_model(self):
        """Register model watches to be able to react to state changes."""

        def action_handler(action):
            def state_handler(table, row):
                self.raise_event((action, table.name), row)
                self.raise_event((action, table.name, row.pkey), row)
            return state_handler

        for t in ('nic', 'bond', 'nic_role'):
            self.model[t].on_create_state(action_handler('create'), ['desired'])
            self.model[t].on_update_state(action_handler('update'), ['desired'])
            self.model[t].on_delete_state(action_handler('delete'), ['desired'])
    # /def watch_model

# /class ModelManager


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
