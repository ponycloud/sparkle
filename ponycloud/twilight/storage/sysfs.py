#!/usr/bin/python -tt

"""
Simplistic Python SysFS interface.

Shamelessly stolen from:
    http://stackoverflow.com/questions/4648792/
"""

__all__ = ['sys', 'proc', 'Node']

from os import listdir
from os.path import isdir, isfile, join, realpath

class Node(object):
	def __init__(self, path='/sys'):
		self.path = realpath(path)

	def __repr__(self):
		return '<sysfs.Node "%s">' % self.path

	def __setitem__(self, name, val):
		path = realpath(join(self.path, name))
		if isfile(path):
			with open(path, 'w') as fp:
				fp.write(str(val))
		else:
			raise RuntimeError('cannot write to non-files')

	def __getitem__(self, name):
		path = realpath(join(self.path, name))
		if isfile(path):
			with open(path, 'r') as fp:
				data = fp.read().strip()
			try:
				return int(data)
			except ValueError:
				return data
		elif isdir(path):
			return Node(path)

	def __iter__(self):
		return iter(listdir(self.path))


sys = Node('/sys')
proc = Node('/proc')

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-