#!/usr/bin/python -tt

__all__ = ['brctl', 'BrctlError']


from subprocess import Popen, PIPE
import re


class BrctlError(Exception):
    """Brctl-related exception."""


class Brctl(object):
    """
    Brctl wrapper.
    """

    def __call__(self, argv):
        """
        Calls brctl command specified by the argv.

        If possible, returns parsed output.
        """

        # Start the ip process.
        p = Popen(['brctl'] + argv, stdout=PIPE, stderr=PIPE, \
                                    close_fds=True, bufsize=-1)

        # Check it's status.
        if 0 != p.wait():
            raise BrctlError(p.stderr.read().strip())

        # Return parsed output.
        for i in xrange(len(argv), 0, -1):
            parser = '_parse_%s' % '_'.join([a for a in argv[:i] \
                                               if not a.startswith('-')])
            if hasattr(self, parser):
                return getattr(self, parser)(p.stdout.read().strip())

        return p.stdout.read().strip()
    # /def __call__

# /class IP

brctl = Brctl()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
