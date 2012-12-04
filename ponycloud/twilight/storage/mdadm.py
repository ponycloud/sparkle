#!/usr/bin/python -tt

__all__ = ['mdadm', 'MdadmError']

from cmdwrapper import CmdWrapper, CmdError

class MdadmError(CmdError):
	"""Mdadm-related exception."""


class Mdadm(CmdWrapper):
	"""
	Madadm wrapper.
	"""
	command_name = 'mdadm'
	error = MdadmError

# /class Mdadm

mdadm = Mdadm()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-