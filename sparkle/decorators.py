#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['synchronized']

from functools import wraps

def synchronized(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return fn(self, *args, **kwargs)
    return wrapper

# vim:set sw=4 ts=4 et:
