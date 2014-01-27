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

__all__ = ['Pointer', 'apply_patch', 'validate_patch', 'split']


import jsonschema
import os.path
import yaml

from collections import Iterable, Mapping, MutableMapping


with open(os.path.dirname(__file__) + '/patch.schema.yaml') as fp:
        patch_schema = yaml.load(fp)


def unescape(part):
    part = part.replace('~1', '/')
    part = part.replace('~0', '~')
    return part


def split(path):
    if '/' != path[:1]:
        raise ValueError('invalid path %r' % (path,))
    return [unescape(part) for part in path.split('/')[1:]]


def cast_part(document, part):
    if isinstance(document, Mapping):
        return part
    elif isinstance(document, basestring):
        raise KeyError('path (%r) points into scalar (%r)' % (part, document))
    elif isinstance(document, Iterable):
        if '-' == part:
            return len(document)
        else:
            return int(part)
    else:
        raise KeyError('path (%r) points into scalar (%r)' % (part, document))


class Pointer(object):
    """Pointer to a document fragment."""

    def __init__(self, document, path=[]):
        self.path = []
        self.target = {'': document}

        if isinstance(path, basestring):
            parts = [''] + split(path)
        else:
            parts = [''] + path

        for part in parts[:-1]:
            part = cast_part(self.target, part)
            self.target = self.target[part]
            self.path.append(part)

        part = cast_part(self.target, parts[-1])
        self.path.append(part)
        self.path.pop(0)

        self.key = part

    def get(self):
        """Retrieve the pointed-to value."""
        return self.target[self.key]

    def remove(self):
        """Remove the pointed-to value."""
        if 0 == len(self.path):
            raise TypeError('cannot remove root')
        del self.target[self.key]

    def add(self, value):
        """Add value at the pointed-to location."""

        if 0 == len(self.path):
            raise TypeError('root already exists')

        if isinstance(self.target, Mapping):
            if hasattr(self.target, 'add'):
                self.target.add(self.key, value)
            elif self.key in self.target:
                raise KeyError('key %r already exists' % self.key)
            else:
                self.target[self.key] = value
        else:
            self.target.insert(self.key, value)

    def replace(self, value):
        """Replace the value at the pointed-to location."""

        if hasattr(self.target, 'replace'):
            self.target.replace(self.key, value)
        else:
            self.remove()
            self.add(value)

    def cut(self):
        """
        Extract value at the pointed-to location.
        The extracted value must be pasted elsewhere.
        """

        if hasattr(self.target, 'cut'):
            self.target.cut(self.key)
        else:
            self.remove()

    def paste(self, value):
        """Paste value previously extracted using the cut function."""

        if hasattr(self.target, 'paste'):
            self.target.paste(self.key, value)
        else:
            self.add(value)


def validate_patch(operations):
    """Validate JSON Patch against a schema."""
    jsonschema.validate(operations, patch_schema)


def apply_patch(document, operations):
    """
    Apply sequence of JSON Patch operations to a document.
    The document is always modified in-place.

    Use validate_patch() to make sure the patch makes sense.

    :param document:    JSON-like value to operate on.
    :param operations:  Sequence of JSON Patch operations as per RFC 6902.
    """

    for item in operations:
        op = item['op']
        path = item['path']

        ptr = Pointer(document, path)

        if op in ('test', 'add', 'replace'):
            value = item['value']

        if op in ('move', 'copy'):
            from_ = item['from']

            from_ptr = Pointer(document, from_)

            if len(from_ptr.path) < len(ptr.path):
                if ptr.path[:len(from_ptr.path)] == from_ptr.path:
                    raise ValueError('%r is a prefix of %r' % (from_, path))

        if op == 'test':
            if ptr.get() != value:
                raise ValueError('value of %r does not match' % (ptr.path,))
        elif op == 'remove':
            ptr.remove()
        elif op == 'add':
            ptr.add(value)
        elif op == 'replace':
            ptr.replace(value)
        elif op == 'move':
            ptr.paste(from_ptr.cut())
        elif op == 'copy':
            ptr.add(from_ptr.get())


# vim:set sw=4 ts=4 et:
