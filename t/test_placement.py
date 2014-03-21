#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

import os.path
import pytest
import yaml

import jsonschema

from glob import glob
from sparkle.placement import Placement
from sparkle.model import Model, OverlayModel, Row
from sparkle.schema import schema


class MockHost(object):
    def __init__(self):
        self.desired = {}


class MockManager(object):
    def __init__(self):
        self.hosts = {}
        self.rows = {}

        self.model = Model()
        self.overlay = OverlayModel(self.model)
        self.overlay.add_callback(self.on_commit)

        self.placement = Placement(self)

    def update_placement(self, hosts, name, pkey):
        row = (name, pkey)

        for host in self.rows.get(row, set()).difference(set(hosts)):
            host = self.hosts.setdefault(host, MockHost())
            host.desired.setdefault(name, set()).discard(pkey)

            if not host.desired[name]:
                del host.desired[name]

        self.rows[row] = hosts

        if not hosts:
            del self.rows[row]

        for host in hosts:
            host = self.hosts.setdefault(host, MockHost())
            host.desired.setdefault(name, set()).add(pkey)

    def on_commit(self, rows):
        damaged = set()
        hosts = set()

        for old, new in rows:
            for row in self.placement.damage(old):
                damaged.add((row.table.name, row.pkey))

        yield

        for name, pkey in damaged:
            hosts = set(self.placement.repair(Row(self.model[name], pkey)))
            self.update_placement(hosts, name, pkey)


def make_test(case, test):
    def test_runner():
        manager = MockManager()

        for i, step in enumerate(test.get('steps', [])):
            changes = []

            for name, pkey, state, part in step.get('update', []):
                if isinstance(pkey, basestring):
                    changes.append((name, pkey, state, part))
                else:
                    changes.append((name, tuple(pkey), state, part))

            manager.overlay.load(changes)
            manager.overlay.commit()

            if 'expect' in step:
                for host in set(manager.hosts).union(step['expect']):
                    items = step['expect'].get(host, [])
                    expected = set(tuple(row) for row in items)
                    desired = manager.hosts.get(host, MockHost()).desired
                    placed = set()
                    for name, pkeys in desired.iteritems():
                        for pkey in pkeys:
                            placed.add((name, pkey))

                    missing = expected.difference(placed)
                    unexpected = placed.difference(expected)

                    assert not missing, 'step %i: host %r: missing %r' % (i, host, list(missing))
                    assert not unexpected, 'step %i: host %r: unexpected %r' % (i, host, list(unexpected))

    test_runner.__name__ = 'test_' + case
    test_runner.__doc__ = test.get('about')
    return test_runner


with open(__file__[:-3] + '.schema.yaml') as fp:
    suite_schema = yaml.load(fp)


for name in glob(os.path.dirname(__file__) + '/placement/*.yaml'):
    with open(name) as fp:
        suite = yaml.load(fp)

    jsonschema.validate(suite, suite_schema)

    for name, test in suite.iteritems():
        runner = make_test(name, test)
        vars()[runner.__name__] = runner


# vim:set sw=4 ts=4 et:
