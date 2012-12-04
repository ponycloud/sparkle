#!/usr/bin/python -tt

__all__ = ['lvm', 'LvmError']

from cmdwrapper import CmdWrapper, CmdError

def to_int(value):
	try:
		return int(value)
	except ValueError:
		return value

class LvmError(CmdError):
	"""Dmsetup-related exception."""

class LvmWrapper(CmdWrapper):
	VGDISPLAY_KEYS = [
		'vg_name',
		'vg_access',
		'vg_status',
		'vg_number',
		'maximum_number_of_lvms',
		'current_number_of_lvms',
		'open_count_of_all_lvms',
		'max_lvm_size',
		'max_num_of_phys_vols',
		'cur_num_of_phy_vols',
		'act_num_of_phy_vols',
		'size_of_vg_in_kb',
		'phys_ext_size',
		'phys_ext',
		'alloc_phys_ext',
		'free_phys_ext',
		'uuid_of_vg'
	]

	LVDISPLAY_KEYS = [
		'lv_name',
		'volume_group_name',
		'lv_access',
		'lv_status',
		'internal_lv_number',
		'open_count_of_lv',
		'lv_size_in_sectors',
		'cur_extents',
		'alloc_extents',
		'alloc_policy',
		'read_ahead_sect',
		'major',
		'minor'
	]

	command_name = 'lvm'
	error = LvmError

	def _parse_vgdisplay(self, input):
		lines = [[to_int(col.strip()) for col in line.split(':')] for line in input.split('\n')]
		vgs = {}
		for line in lines:
			vgs[line[0]] = dict(zip(self.VGDISPLAY_KEYS, line))
		return vgs
	
	def _parse_lvdisplay(self, input):
		lines = [[to_int(col.strip()) for col in line.split(':')] for line in input.split('\n')]
		lvs = {}
		for line in lines:
			lvs[line[0]] = dict(zip(self.LVDISPLAY_KEYS, line))
		return lvs

	def _parse_pvs(self, input):
		lines = [[col.strip() for col in line.split(':')] for line in input.split('\n')]
		pvs = {}
		for line in lines:
			pvs[line[0]] = line
		return pvs

lvm = LvmWrapper()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-