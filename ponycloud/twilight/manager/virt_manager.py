#!/usr/bin/python -tt

from ponyvirt import Hypervisor

class VirtManager(object):
    """Manager mixin that takes care about communication with libvirt."""

    def __init__(self):
        # Connect to the hypervisor.
        self.virt = Hypervisor()

        # Query host uuid, it is used everywhere.
        self.uuid = self.virt.sysinfo['system']['uuid'].lower()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
