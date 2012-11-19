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
        # This will actually send out the identity to Sparkle.
        self.model = Model()
        self.model.load([('host', self.uuid, 'current', {'uuid': self.uuid})])


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


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
