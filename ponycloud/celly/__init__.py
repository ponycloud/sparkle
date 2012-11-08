#!/usr/bin/python -tt

__all__ = ['Celly']

from httplib2 import Http
from os.path import dirname

import cjson
import re


def guess_key(item):
    for part in ('desired', 'current'):
        for key in ('uuid', 'id', 'key', 'hash', 'hwaddr', 'email'):
            if part in item and key in item[part]:
                return item[part][key]


class CollectionProxy(object):
    """
    Remote collection proxy.
    """

    def __init__(self, celly, uri, children={}):
        self.children = children
        self.celly = celly
        self.uri = uri

    def __iter__(self):
        out = []
        for item in self.celly.request(self.uri)['items']:
            child_uri = '%s%s' % (self.uri, guess_key(item))
            out.append(EntityProxy(self.celly, child_uri, self.children))

        return iter(out)

    def __getitem__(self, key):
        child_uri = '%s%s' % (self.uri, key)
        return EntityProxy(self.celly, child_uri, self.children)

    def post(self, desired):
        result = self.celly.request(self.uri, 'POST', cjson.encode(desired))
        child_uri = '%s%s' % (self.uri, guess_key({'desired': result}))
        return EntityProxy(self.celly, child_uri, self.children)

    def __repr__(self):
        return '<CollectionProxy %s>' % self.uri


class EntityProxy(object):
    """
    Remote entity proxy.
    """

    def __init__(self, celly, uri, children={}):
        self.celly = celly
        self.uri = uri

        for name, child in children.items():
            child_uri = '%s/%s/' % (self.uri, name.replace('_', '-'))
            setattr(self, name, CollectionProxy(self.celly, child_uri, child))

    @property
    def desired(self):
        return self.celly.request(self.uri).get('desired')

    @desired.setter
    def desired(self, value):
        self.celly.request(self.uri, 'PUT', cjson.encode(value))

    @property
    def current(self):
        return self.celly.request(self.uri).get('current')

    def delete(self):
        return self.celly.request(self.uri, 'DELETE')

    def __repr__(self):
        return '<EntityProxy %s>' % self.uri


class Celly(object):
    """
    Ponycloud RESTful API client.
    """

    def __init__(self, base_uri):
        """Queries the API and constructs client accordingly."""
        self.uri = base_uri
        self.http = Http()
        self.children = {}

        for ep in self.endpoints:
            c = self.children
            for name in [dirname(x) for x in re.split('>/', ep[1:])]:
                c = c.setdefault(name.replace('-', '_'), {})

        for name, child in self.children.items():
            child_uri = '%s/%s/' % (self.uri, name.replace('_', '-'))
            setattr(self, name, CollectionProxy(self, child_uri, child))

    def request(self, uri, method='GET', body=None, headers=None):
        status, data = self.http.request(uri, method=method, body=body, \
                                              headers=headers)

        if status['content-type'] == 'application/json':
            data = cjson.decode(data)

        if '200' == status['status']:
            return data

        if '404' == status['status']:
            if isinstance(data, dict):
                raise KeyError(data['message'])
            raise KeyError('not found')

        if '400' == status['status']:
            if isinstance(data, dict):
                raise ValueError(data['message'])
            raise ValueError('bad request')

        if isinstance(data, dict):
            raise Exception(data['message'])
        raise Exception('API request failed')

    @property
    def endpoints(self):
        return self.request('%s/_endpoints' % self.uri)


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
