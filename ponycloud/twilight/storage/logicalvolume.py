#!/usr/bin/python -tt

__all__ = ['LogicalVolume']

from lvm import lvm

class LogicalVolume(object):

	def __init__(self, vgname, name):
		self.name = name
		self.vgname = vgname

	@property
	def lvname(self):
		return '%s/%s' % (self.vgname, self.name)

	@classmethod
	def create(cls, vgname, name, size):
		lvm(['lvcreate', '--name', name, '--size', size, vgname])
		return cls(vgname, name)

	def display(self):
		return lvm(['lvdisplay', '-c', self.lvname])

	def remove(self):
		return lvm(['lvremove', '--force', self.lvname])

	def resize(self, size_change):
		"""
		Resizes this logical volume according to size_change
		so to eg. add 5 GB you just pass +5G
		"""
		return lvm(['lvresize', '--size', size_change, self.lvname])

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-