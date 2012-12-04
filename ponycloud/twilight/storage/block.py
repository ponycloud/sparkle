#!/usr/bin/python -tt

__all__ = ['Block']

from sysfs import sys, proc, Node
from udev import udevctx, Device

class Block(object):
	def __init__(self, name):
		self.name = name
		self.node = sys['class']['block'][name]

	@property
	def size(self):
		"""
		Size in bytes
		"""
		return self.node['size'] * 512

	@property
	def holders(self):
		"""
		Name of devices that are using this one
		"""
		return list(self.node['holders'])

	@property
	def device_path(self):
		if not hasattr(self, '_device_path'):
			setattr(self, '_device_path', Device.from_name(udevctx, 'block', self.name)['DEVNAME'])
		return self._device_path

	@property
	def slaves(self):
		"""
		Names of enslaved devices
		"""
		return list(self.node['slaves'])

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-