#!/usr/bin/python -tt

__all__ = ['make_event_handler']

def make_event_handler(manager):
    """
    Creates function that forwards relevant events to manager.
    """

    def event_handler(data, sender):
        """
        Event handler bound to a manager instance.
        """

        if data.get('event') == 'sparkle-resync':
            return manager.sparkle_resync()

        if data.get('event') == 'sparkle-state-update':
            del data['event']
            return manager.sparkle_state_update(**data)

        # Print unknown events.
        print 'event', data

    # Return the parameterized event handler.
    return event_handler

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
