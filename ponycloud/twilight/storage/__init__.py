#!/usr/bin/python -tt

from block import *
from disk import *
from raid import *
from partition import *
from dmdevice import *
from volumegroup import *
from logicalvolume import *

from sysfs import sys, proc, Node

class Storage(object):

	def __iter__(self):
		"""Iterates over names of system storage devices."""
		return iter(list(sys['class']['block']))


	def __getitem__(self, name):
		"""Retrieves interface proxy object, guessing storage devices."""

		if sys['class']['block'][name]['device']:
			return Disk(name)

		if sys['class']['block'][name]['dm']:
			return DmDevice(name)

		if sys['class']['block'][name]['md']:
			return Raid(name)

		if sys['class']['block'][name]['partition']:
			return Partition(name)

		return Block(name)


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-