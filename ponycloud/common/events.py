#!/usr/bin/python -tt

__all__ = ['EventMixin']

from twisted.internet import reactor
from functools import wraps

class EventMixin(object):
    """
    Mixin for event-driven processing.

    Just inherit from it and use it's methods to add event-driven
    programming capabilities to the class.

    Events are identified by arbitrary hashable objects, usually just
    strings or tuples. Event can be fired or cancelled. If the event is
    fired, associated handlers might be executed (once all events they
    depend on have been fired) or it might be cancelled, in which case
    the counts for all interested handlers are reset and one-time
    handlers are removed.

    Handlers are not run immediately, they are queued using Twisted's
    `reactor.callLater()` function, so don't rely on them being done
    right after you raise the respective events.
    """

    def raise_event(self, event, value=True):
        """Raise specified event."""

        print 'RAISE', event, value

        # Get handlers for that particular event.
        handlers = getattr(self, '_events', {}).get(event, set())

        for h in list(handlers):
            # Store result for this event.
            h.wait_for[event] = value

            if None not in h.wait_for.values():
                # Collect arguments.
                args = [h.wait_for[k] for k in h.wait_for_keys]

                # Queue the handler.
                reactor.callLater(0, h, *args)

                # Reset the results collected so far and remove
                # all one-time handlers.
                for ev in h.wait_for:
                    h.wait_for[ev] = None
                    if h.run_once:
                        getattr(self, '_events', {}).get(ev, set()).discard(h)
    # /def raise_event


    def cancel_event(self, event):
        """Cancel specified event."""

        print 'CANCEL', event

        # Get handlers for that particular event.
        handlers = getattr(self, '_events', {}).get(event, set())

        for h in list(handlers):
            for ev in h.wait_for:
                h.wait_for[ev] = None
                if h.run_once:
                    getattr(self, '_events', {}).get(ev, set()).discard(h)
    # /def cancel_event


    def on_events(self, events, handler, once=False):
        """Add handler for given list of events."""

        if not hasattr(self, '_events'):
            setattr(self, '_events', {})

        # Create wrapper to prevent modification of the original handler.
        @wraps(handler)
        def wrapper(*args, **kwargs):
            return handler(*args, **kwargs)

        wrapper.wait_for = {}
        wrapper.wait_for_keys = list(events)
        wrapper.run_once = once

        for ev in events:
            wrapper.wait_for[ev] = None
            self._events.setdefault(ev, set()).add(wrapper)
    # /def on_events


    def handle_events(self, events, once=False):
        """
        Decorator for on_events().

        Usage::

            def feed_friend(self):
                @self.handle_events(['cooking-done', 'friend-came'], once=True)
                def serve_food(food_type):
                    print 'Here comes %s!' % food_type

                @self.handle_events(['kitchen-on-flames'], once=True):
                def restart_cooking(on_flames):
                    print "Ooops!  We'll just have to order something..."
                    self.cancel_event('cooking-done')
        """

        def decorate(fn):
            self.on_events(events, fn, once)
            return fn

        return decorate
    # /def handle_events
# /class EventMixin

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
