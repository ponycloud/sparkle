#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

__doc__ = """
JSON Patch

Custom JSON Patch implementation for our database-to-dict proxy objects.
Apart from the usual dict operations it supports custom methods for
correct addition, replacement and moving values around.

The "copy" operation does not perform any kind of copying, it just adds
another reference to original data.  That, however, does not mean that the
target won't copy the referenced value when receiving it.
"""

__all__ = ['Pointer', 'normalize_path']


from sparkle.common import *

from collections import Iterable, Mapping, MutableMapping


def unescape(part):
    part = part.replace('~1', '/')
    part = part.replace('~0', '~')
    return part


def normalize_path(path):
    """
    Normalize string or list path to the list path.
    """

    if isinstance(path, list):
        return path

    if isinstance(path, basestring):
        if '/' != path[:1]:
            raise DataError('path missing the leading /', path)
        return [unescape(part) for part in path.split('/')][1:]

    raise DataError('invalid path', path)


class Pointer(object):
    """Pointer to a document fragment."""

    def __init__(self, parent, path=[]):
        """
        Prepare pointer to an element of given parent.
        Path is mostly informative, only the last element is used.
        """

        assert isinstance(parent, MutableMapping), \
               'parent must be a MutableMapping'

        self.parent = parent
        self.path = path


    @property
    def key(self):
        """
        Last element of the internal path for convenience.
        Will fail if the path is of zero length.
        """

        if not self.path:
            raise DataError('cannot operate on the absolute root', self.path)

        return self.path[-1]


    def relative(self, path):
        """
        Return Pointer to child element using given path.
        """

        path = normalize_path(path)
        parent = self.parent
        sofar = self.path[:-1]

        for elem in (self.path[-1:] + path)[:-1]:
            sofar.append(elem)

            try:
                parent = parent[elem]
            except KeyError:
                raise PathError('not found', sofar)

        return Pointer(parent, self.path + path)


    def get(self):
        """
        Retrieve the pointed-to value.
        """

        try:
            return self.parent[self.key]
        except KeyError:
            raise PathError('not found', self.path)


    def remove(self):
        """
        Remove the pointed-to value.
        """

        try:
            del self.parent[self.key]
        except KeyError:
            raise PathError('not found', self.path)
        except (ValueError, TypeError), e:
            raise DataError(e.message, self.path)


    def add(self, value):
        """
        Add value at the pointed-to location.
        """

        try:
            if hasattr(self.parent, 'add'):
                self.parent.add(self.key, value)
            elif self.key in self.parent:
                raise DataError('path already exists', self.path)
            else:
                self.parent[self.key] = value
        except KeyError:
            raise PathError('not found', self.path)
        except (ValueError, TypeError), e:
            raise DataError(e.message, self.path)


    def replace(self, value):
        """
        Replace the value at the pointed-to location.
        """

        if hasattr(self.parent, 'replace'):
            try:
                self.parent.replace(self.key, value)
            except KeyError:
                raise PathError('not found', self.path)
            except (ValueError, TypeError), e:
                raise DataError(e.message, self.path)
        else:
            self.remove()
            self.add(value)


    def cut(self):
        """
        Extract value at the pointed-to location.
        The extracted value must be pasted elsewhere.
        """

        if hasattr(self.parent, 'cut'):
            try:
                self.parent.cut(self.key)
            except KeyError:
                raise PathError('not found', self.path)
            except (ValueError, TypeError), e:
                raise DataError(e.message, self.path)
        else:
            self.remove()


    def paste(self, value):
        """
        Paste value previously extracted using the cut function.
        """

        if hasattr(self.parent, 'paste'):
            try:
                self.parent.paste(self.key, value)
            except KeyError:
                raise PathError('not found', self.path)
            except (ValueError, TypeError), e:
                raise DataError(e.message, self.path)
        else:
            self.add(value)


    def patch(self, ops):
        """
        Apply a complete patch with paths being relative to the Pointer.

        The patch must be a valid JSON Patch document: no additional error
        checking is performed.
        """

        for item in ops:
            op = item['op']
            path = item['path']

            dst = self.relative(path)

            if op in ('test', 'add', 'replace'):
                value = item['value']

            if op in ('move', 'copy'):
                src = self.relative(item['from'])

                if len(src.path) < len(dst.path):
                    if dst.path[:len(src.path)] == src.path:
                        raise DataError('from is a prefix of path', src.path)

            if op == 'test':
                if dst.get() != value:
                    raise ConflictError('value test failed', dst.path)
            elif op == 'remove':
                dst.remove()
            elif op == 'add':
                dst.add(value)
            elif op == 'replace':
                dst.replace(value)
            elif op == 'move':
                dst.paste(src.cut())
            elif op == 'copy':
                dst.add(src.get())


# vim:set sw=4 ts=4 et:
