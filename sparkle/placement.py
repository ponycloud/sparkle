#!/usr/bin/python -tt
# -*- coding: utf-8 -*-


class Placement(object):
    """Encapsulates placement algorithms."""

    def __init__(self, manager):
        """Remember the manager."""
        self.manager = manager


    def on_row_changed(self, old, new):
        """
        Triggered on every row change.
        """

        handler = 'on_' + old.table.name + '_changed'
        if hasattr(self, handler):
            return getattr(self, handler)(old, new)
        else:
            print 'no placement routine for %s' % (old.table.name,)


    def on_host_changed(self, old, new):
        """
        By default, hosts are placed on "themselves" for "themselves".
        """

        if new.desired is None:
            self.manager.withdraw(new.pkey, new, new)
        else:
            self.manager.bestow(new.pkey, new, new)


# vim:set sw=4 ts=4 et:
