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

    @property
    def rows(self):
        result = {}

        for host, rows in self.placement.iteritems():
            for row, owner in rows:
                result.setdefault(row, set()).add(host)

        return result

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


def make_test(case, test):
    def test_runner():
        manager = MockManager()
        placement = Placement(manager)
        model = Model()
        overlay = OverlayModel(model)

        overlay.add_callback(placement.on_row_changed)

        for i, step in enumerate(test.get('steps', [])):
            changes = []

            for name, pkey, state, part in step.get('update', []):
                if isinstance(pkey, basestring):
                    changes.append((name, pkey, state, part))
                else:
                    changes.append((name, tuple(pkey), state, part))

            overlay.load(changes)
            overlay.commit()

            if 'expect' in step:
                for host in set(manager.placement).union(step['expect']):
                    items = step['expect'].get(host, [])
                    expected = set(tuple(row) for row in items)
                    placed = set(x[0] for x in manager.placement.get(host, set()))

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
