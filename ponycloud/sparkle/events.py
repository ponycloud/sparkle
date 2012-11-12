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

        # Ignore bogus events.
        if not isinstance(data, dict):
            print 'bogus event', data
            return

        # Drop host information for now.
        if data.get('event') == 'twilight-state-update':
            return manager.twilight_state_update(data, sender)

        # Print unknown events.
        print 'event', data

    # Return the parameterized event handler.
    return event_handler

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
