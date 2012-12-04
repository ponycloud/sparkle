#!/usr/bin/python -tt

__all__ = ['Partition']

from block import Block
from udev import Device, udevctx

from sysfs import sys

import parted

class Partition(Block):

	@classmethod
	def create(cls, device_path, start, end):
		"""
		Device_path is something like /dev/sda
		start and end are integers of start/end in bytes
		"""

		if start < 0:
			raise ValueError('partition must start at a positive offset')
		if end <= start:
			raise ValueError('partition size must be positive')

		device_name = device_path.split('/')[-1]
		device = parted.Device(device_path)
		disk = parted.freshDisk(device, 'msdos')

		constraint = parted.Constraint(device=device)
		# Geometry units are sectors <===#
		block_size = int(sys['class']['block'][device_name]['queue']['logical_block_size'])
		start_in_sectors = start / block_size
		end_in_sectors = end / block_size

		geometry = parted.Geometry(device=device, start=start_in_sectors, end=end_in_sectors)
		filesystem = parted.FileSystem(type="ext4", geometry=geometry)

		# Create the partition object using the objects we defined before
		partition = parted.Partition(disk=disk, fs=filesystem, type=parted.PARTITION_NORMAL, geometry=geometry)
		# Redefine constraint to snap to the exact limits
		constraint = parted.Constraint(exactGeom=geometry)

		# Add partition to the disk. Will return True if successful
		disk.addPartition(partition=partition, constraint=constraint)
		# All the stuff we just did needs to be committed to the disk.
		disk.commit()
		return cls(partition.getDeviceNodeName())

	@property
	def parent_disk(self):
		"""
		The device path (eg. /dev/sdd) the partition is on
		"""
		if not hasattr(self, '_parent_disk'):
			disk_name = Device.from_name(udevctx, 'block', self.name)['DEVPATH'].split('/')[-2]
			setattr(self, '_parent_disk', Device.from_name(udevctx, 'block', disk_name)['DEVNAME'])
		return self._parent_disk

	def _get_partition_by_name(self, disk, partition_name):
		return [x for x in disk.partitions if x.getDeviceNodeName() == partition_name].pop()

	def destroy(self):
		"""
		Destroys this partition
		"""
		device = parted.Device(self.parent_disk)
		disk = parted.Disk(device)
		disk.deletePartition(self._get_partition_by_name(disk, self.name))
		return disk.commit()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-