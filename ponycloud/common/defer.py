#!/usr/bin/python -tt

__all__ = ['spawn_and_call_back']

def spawn_and_call_back(function, callback):
    """
    Returns wrapper that will call given function in a parallel thread
    with specified callback receiving the result back in the main thread
    when the function returns.

    Usage::
        spawn_and_call_back(slow_function, callback)(42, 'foo')
    """
    def wrapper(*args, **kwargs):
        d = threads.deferToThread(function, *args, **kwargs)
        d.addCallback(callback)
        return d
    return wrapper

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
