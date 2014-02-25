#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['call_sync', 'remove_nulls']

from twisted.internet.threads import blockingCallFromThread
from twisted.internet import reactor
from collections import Mapping
from uuid import uuid4


def call_sync(fn, *args, **kwargs):
    """Perform blockingCallFromThread on default reactor."""
    return blockingCallFromThread(reactor, fn, *args, **kwargs)


def remove_nulls(data):
    """
    Recursively remove None values from dictionary.
    """

    if not isinstance(data, Mapping):
        return data

    return {k: remove_nulls(v) for k, v in data.iteritems() if v is not None}


# vim:set sw=4 ts=4 et:
