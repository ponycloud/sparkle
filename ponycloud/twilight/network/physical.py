#!/usr/bin/python -tt

from iface import Interface
from sysfs import sys, proc

import re

class Physical(Interface):
    """Wraps physical network interfaces."""

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
