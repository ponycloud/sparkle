#!/usr/bin/python -tt

__all__ = ['Disk']

from block import Block
from udev import udevctx, Device

class Disk(Block):
	@property
	def partitions(self):
		"""
		List of partition names
		"""
		return [p for p in self.node if p.startswith(self.name)]

	@property
	def identifier(self):
		"""
		Unique identifier for a disk device.
		"""
		if not hasattr(self, '_identifier'):
			device = Device.from_name(udevctx, 'block', self.name)
			if device.get('ID_BUS') == 'ata':
				setattr(self, '_identifier', 'ata-%s' % device['ID_SERIAL'])
			elif device.get('ID_WWN'):
				setattr(self, '_identifier', 'wwn-%s' % device['ID_WWN'])
			elif device.get('DM_UUID'):
				setattr(self, '_identifier', 'dm-%s' % device['DM_UUID'])
			elif device.get('ID_SCSI_SERIAL'):
				setattr(self, '_identifier', 'scsi-%s' % device['ID_SCSI_SERIAL'])
			elif 'virtio' in device.get('ID_PATH', ''):
				setattr(self, '_identifier', 'virtio-%s' % device['ID_SERIAL'])
			elif 'ID_SERIAL' in device:
				setattr(self, '_identifier', 'unknown-%s' % device['ID_SERIAL'])
			else:
				raise RuntimeError('unknown disk type')
		return self._identifier

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-