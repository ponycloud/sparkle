#!/usr/bin/python -tt

__all__ = ['CmdWrapper', 'CmdError']

from subprocess import Popen, PIPE

class CmdError(Exception):
	"""CmdWrapper-related exception."""

class CmdWrapper(object):
	"""
	Generic command wrapper.
	"""
	command_name = ''
	error = CmdError

	def __call__(self, argv, input=None):
		"""
		Calls Dmsetup command specified by the argv.

		If possible, returns parsed output.
		"""

		# Start the mdadm process.
		argv = [str(a) for a in argv]
		p = Popen([self.command_name] + argv, stdout=PIPE, stdin=PIPE, stderr=PIPE,\
				  close_fds=True, bufsize=-1)

		stdout, stderr = p.communicate(input=input)
		code = p.wait()

		if code != 0:
			raise self.error(stderr.strip())

		# Return parsed output.
		for i in xrange(len(argv), 0, -1):
			parser = '_parse_%s' % '_'.join([a for a in argv[:i]\
											 if not a.startswith('-')])
			if hasattr(self, parser):
				return getattr(self, parser)(stdout.strip())

		return stdout.strip()
	# /def __call__

# /class CmdWrapper

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-