#!/usr/bin/python -tt

__all__ = ['dmsetup', 'DmsetupError']

import re
from cmdwrapper import CmdWrapper, CmdError

class DmsetupError(CmdError):
	"""Dmsetup-related exception."""


class Dmsetup(CmdWrapper):
	"""
	Dmsetup wrapper.
	"""
	command_name = 'dmsetup'
	error = DmsetupError

	def _parse_ls(self, input):
		return [tuple(re.sub('[()]', '', col) for col in line.split('\t')) for line in input.split('\n')]

	def _parse_status(self, input):
		return [[col.strip() for col in line.split(': ', 1)] for line in input.split('\n')]

	def _parse_table(self, input):
		return [[col.strip() for col in line.split(': ', 1)] for line in input.split('\n')]

# /class Dmsetup

dmsetup = Dmsetup()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-