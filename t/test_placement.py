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


class MockManager(object):
    def __init__(self):
        self.placement = {}

    def bestow(self, host, row, owner=None):
        if isinstance(row, Row):
            row = (row.table.name, row.pkey)

        if isinstance(owner, Row):
            owner = (owner.table.name, owner.pkey)

        if owner is None:
            owner = row

        self.placement.setdefault(host, set()).add((row, owner))

    def withdraw(self, host, row, owner=None):
        if isinstance(row, Row):
            row = (row.table.name, row.pkey)

        if isinstance(owner, Row):
            owner = (owner.table.name, owner.pkey)

        if owner is None:
            owner = row

        self.placement.setdefault(host, set()).discard((row, owner))

        if not self.placement[host]:
            del self.placement[host]

    def withdraw_all(self, row, owner=None):
        for host in self.placement:
            self.withdraw(host, row, owner)


def make_test(name, test):
    def test_runner():
        manager = MockManager()
        placement = Placement(manager)
        model = Model()
        overlay = OverlayModel(model)

        overlay.add_callback(placement.on_row_changed)

        for step in test.get('steps', []):
            changes = []
            for tname, table in step.get('update', {}).iteritems():
                for row in table:
                    for part in ('desired', 'current'):
                        if part in row:
                            if isinstance(schema.tables[tname].pkey, basestring):
                                pkey = row[part][schema.tables[tname].pkey]
                            else:
                                pkey = tuple(row[part][k] for k in schema.tables[tname].pkey)

                            changes.append((tname, pkey, part, row[part]))

            overlay.load(changes)
            overlay.commit()

            if 'expect' in step:
                for host in set(manager.placement).union(step['expect']):
                    items = step['expect'].get(host, [])
                    expected = set(tuple(row) for row in items)
                    placed = set(x[0] for x in manager.placement.get(host, set()))

                    missing = expected.difference(placed)
                    unexpected = placed.difference(expected)

                    assert not missing, 'host %r: missing %r' % (host, missing)
                    assert not unexpected, 'host %r: unexpected %r' % (host, unexpected)

    test_runner.__name__ = 'test_' + name
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
