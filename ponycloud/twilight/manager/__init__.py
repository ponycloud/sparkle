#!/usr/bin/python -tt

__all__ = ['Manager']


from virt_manager import VirtManager
from model_manager import ModelManager
from udev_manager import UdevManager
from network_manager import NetworkManager

from ponycloud.common.events import EventMixin


class Manager(VirtManager, ModelManager, UdevManager, NetworkManager, \
              EventMixin):
    """
    The main application logic of Twilight.

    The class actually merges several portions of functionality in order
    to increase readability of the code. Look at parent classes to get
    the idea.
    """

    def __init__(self, sparkle):
        """Call parent constructors."""

        VirtManager.__init__(self)
        ModelManager.__init__(self, sparkle)
        UdevManager.__init__(self)
        NetworkManager.__init__(self)


    def start(self):
        """Starts periodic tasks."""
        self.start_model_tasks()
        self.start_udev_tasks()


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
