#!/usr/bin/python -tt

__all__ = ['ip', 'IPRouteError']


from subprocess import Popen, PIPE
import re


class IPRouteError(Exception):
    """IPRoute2-related exception."""


def parse_opts(parts):
    """
    Parse "[key, value, key, value]" option pairs with integer detection.
    """

    kvs = {}

    for k, v in zip(parts[0::2], parts[1::2]):
        try:
            v = int(v)
        except ValueError:
            pass

        kvs[k] = v

    return kvs
# /def parse_opts


class IP(object):
    """
    IPRoute wrapper.
    """

    def __call__(self, argv):
        """
        Calls IPRoute ip command specified by the argv.

        If possible, returns parsed output.
        """

        # Start the ip process.
        p = Popen(['ip', '-o'] + argv, stdout=PIPE, stderr=PIPE, \
                                       close_fds=True, bufsize=-1)

        # Check it's status.
        if 0 != p.wait():
            raise IPRouteError(p.stderr.read().strip())

        # Return parsed output.
        for i in xrange(len(argv), 0, -1):
            parser = '_parse_%s' % '_'.join([a for a in argv[:i] \
                                               if not a.startswith('-')])
            if hasattr(self, parser):
                return getattr(self, parser)(p.stdout.read().strip())

        return p.stdout.read().strip()
    # /def __call__


    def _parse_route_show(self, out):
        data = []

        for row in out.split('\n'):
            parts = re.split('[ \\\\\t\r\n]+', row.strip())
            if parts[0] in ('broadcast', 'local', 'unreachable'):
                route = {'type': parts.pop(0), 'route': parts.pop(0)}
            else:
                route = {'route': parts.pop(0)}
            route.update(parse_opts(parts))
            data.append(route)

        return data
    # /def _parse_route_show


    def _parse_addr_show(self, out):
        data = {}

        for row in out.split('\n'):
            parts = re.split('[ \\\\\t\r\n]+', row.strip())

            name = parts[1]
            parent = None
            if ':' in name:
                name = name[:-1]
            if '@' in name:
                name, parent = name.split('@')

            if ':' in parts[1]:
                iface = {'name': name,
                         'parent': parent,
                         'ifindex': int(re.sub('[^0-9]+', '', parts[0])),
                         'flags': set(parts[2][1:-1].split(','))}
                iface.update(parse_opts(parts[3:]))
                data[name] = iface

            else:
                data[name].setdefault(parts[2], [])
                address = {'address': parts[3]}
                address.update(parse_opts(parts[4:]))
                data[name][parts[2]].append(address)

        return data
    # /def _parse_addr_show

    # Link is same as address.
    _parse_link_show = _parse_addr_show

# /class IP

ip = IP()

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
