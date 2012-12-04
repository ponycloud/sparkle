#!/usr/bin/python -tt

__all__ = ['Raid']

from mdadm import mdadm
from block import Block

class Raid(Block):

	@classmethod
	def create(cls, name, level, uuid=None, slaves=[]):
		cmd = ['--create', '-R', '/dev/%s' % name, '-n', str(len(slaves)), '--level=%s' % level]
		if uuid:
			cmd += ['--uuid=%s' % uuid]
		mdadm(cmd + slaves)
		return cls(name)

	@classmethod
	def assemble(cls, name, slaves=[]):
		mdadm(['--assemble', '--force', '/dev/%s' % name] + slaves)
		return cls(name)

	def stop(self):
		mdadm(['--manage', self.device_path, '--stop'])
		return

	def slave_del(self, slave):
		mdadm(['--manage', self.device_path, '--fail', slave, '--remove', slave])

	def slave_add(self, slave):
		mdadm(['--manage', self.device_path, '--add', slave])

	@property
	def degraded(self):
		"""True if the array is degraded"""
		return bool(self.node['md']['degraded'])

	@property
	def level(self):
		"""The level of the array (eg. raid1)"""
		return int(self.node['md']['level'].replace('raid', ''))

	@property
	def state(self):
		"""The state of the array (eg. active)"""
		return self.node['md']['array_state']

	@property
	def sync_action(self):
		"""Pending sync action (eg. resync, idle)"""
		return self.node['md']['sync_action']

	@property
	def raid_disk_count(self):
		"""Returns the count of devices in the array"""
		return self.node['md']['raid_disks']

	@property
	def sync_progress(self):
		"""Returns a tuple of (already synced, size of array)"""
		return tuple([int(x.strip()) for x in self.node['md']['sync_completed'].split('/')])


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-