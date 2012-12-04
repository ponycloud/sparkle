#!/usr/bin/python -tt

__all__ = ['DmDevice']

from disk import Disk
from udev import udevctx, Device
from dmsetup import dmsetup
from os.path import basename

class DmDevice(Disk):

	@classmethod
	def create(cls, dm_name, type, args=[]):
		creator = '_create_%s' % str(type)
		if hasattr(cls, creator):
			return getattr(cls, creator)(dm_name, args)
		else:
			raise RuntimeError('Unknown target type')

	@classmethod
	def _create_linear(cls, dmname, table_lines):
		"""
		Create linear mapping based on table_lines.
		Each line is a tuple in the dmsetup table format:
		([start in sectors], [length in sectors], [type], [source device], [offset in sectors])
		eg.
		('4096', '8192', 'linear', '/dev/dm-2', '4192')
		"""
		dmsetup(['create', dmname, '/dev/stdin'], input='\n'.join([' '.join([str(col) for col in line]) for line in table_lines]))
		devname = Device.from_device_file(udevctx, '/dev/mapper/%s' % dmname)['DEVNAME']
		return cls(basename(devname))

	@classmethod
	def get_by_dmname(cls, dmname):
		for device in udevctx.list_devices(subsystem='block', DEVTYPE='disk'):
			if 'DM_NAME' in device and device['DM_NAME'] == dmname:
				return cls(basename(device['DEVPATH']))

	@property
	def device_path(self):
		if not hasattr(self, '_device_path'):
			self._device_path = Device.from_device_file(udevctx, '/dev/mapper/%s' % self.dmname)['DEVNAME']
		return self._device_path

	@property
	def dmname(self):
		if not hasattr(self, '_dmname'):
			self._dmname = Device.from_name(udevctx, 'block', self.name)['DM_NAME']
		return self._dmname

	@property
	def table(self):
		return dmsetup(['table', self.dmname])

	@property
	def status(self):
		return dmsetup(['status', self.dmname])

	def load(self, table_lines):
		dmsetup(['load', self.dmname, '/dev/stdin'], input='\n'.join(table_lines))
		if hasattr(self, '_device_path'):
			del self._device_path
		return self

	def reload(self, table_lines):
		dmsetup(['reload', self.dmname, '/dev/stdin'], input='\n'.join(table_lines))
		if hasattr(self, '_device_path'):
			del self._device_path
		return self

	def rename(self, new_name):
		dmsetup(['rename', self.dmname, new_name])
		self._dmname = new_name
		return self

	def suspend(self):
		dmsetup(['suspend', self.dmname])
		return self

	def resume(self):
		dmsetup(['resume', self.dmname])
		return self

	def destroy(self):
		return dmsetup(['remove', self.dmname])

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-