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

        if data.get('event') == 'resync':
            return manager.twilight_resync(data['uuid'], sender)

        if data.get('event') == 'update':
            del data['event']
            return manager.twilight_update(sender=sender, **data)

        # Print unknown events.
        print 'event', data

    # Return the parameterized event handler.
    return event_handler

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
