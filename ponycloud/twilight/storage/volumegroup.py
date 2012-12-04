#!/usr/bin/python -tt

__all__ = ['VolumeGroup']

from lvm import lvm, LvmError

class VolumeGroup(object):

	def __init__(self, name):
		self.name = name

	@classmethod
	def create(cls, name, sources=[]):
		lvm(['vgcreate', name] + sources)
		return cls(name)

	def display(self):
		return lvm(['vgdisplay', '-c', self.name])

	def remove(self):
		return lvm(['vgremove', '--force', self.name])

	def reduce(self, pvs):
		return lvm(['vgreduce', self.name] + pvs)

	def extend(self, pvs):
		return lvm(['vgextend', self.name] + pvs)

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-