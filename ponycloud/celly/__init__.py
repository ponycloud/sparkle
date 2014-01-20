#!/usr/bin/python -tt

__all__ = ['Celly']

from httplib2 import Http
from urllib import quote
from os.path import dirname

from simplejson import loads, dumps
import re


class CollectionProxy(object):
    """Remote collection proxy."""

    def __init__(self, celly, uri, prefix=()):
        self.celly = celly
        self.uri = uri
        self.prefix = prefix

    def __iter__(self):
        return iter(self.list)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.list[key]

        child_uri = '%s%s' % (self.uri, quote(key, ''))
        return EntityProxy(self.celly, child_uri, self.prefix)

    def _get_key(self, item):
        if 'desired' in item:
            return item['desired'][self.celly.paths[self.prefix]['pkey']]
        return item['current'][self.celly.paths[self.prefix]['pkey']]

    @property
    def list(self):
        out = []
        for key, value in self.celly.request(self.uri).iteritems():
            child_uri = '%s%s' % (self.uri, quote(key, ''))
            out.append(EntityProxy(self.celly, child_uri, self.prefix))
        return out

    def post(self, data):
        return self.celly.request(self.uri, 'POST', dumps(data))

    def patch(self, ops):
        return self.celly.request(self.uri, 'PATCH', dumps(ops))

    def __repr__(self):
        return '<CollectionProxy %s>' % self.uri


class EntityProxy(object):
    """Remote entity proxy."""

    def __init__(self, celly, uri, prefix=()):
        self.celly = celly
        self.uri = uri

        for path, info in self.celly.iter_child_paths(prefix):
            col_uri = '%s/%s/' % (self.uri, quote(path[-1], ''))
            setattr(self, path[-1], CollectionProxy(self.celly, col_uri, path))

    @property
    def desired(self):
        return self.celly.request(self.uri).get('desired')

    @property
    def current(self):
        return self.celly.request(self.uri).get('current')

    def delete(self):
        return self.celly.request(self.uri, 'DELETE')

    def patch(self, ops):
        return self.celly.request(self.uri, 'PATCH', dumps(ops))

    def __repr__(self):
        return '<EntityProxy %s>' % self.uri


class Celly(object):
    """
    Ponycloud RESTful API client.
    """

    def __init__(self, base_uri='http://127.0.0.1:9860/v1', headers={}):
        """Queries the API schema and constructs client accordingly."""

        self.uri = base_uri
        self.http = Http()
        self.headers = headers

        for path, info in self.iter_child_paths():
            uri = '%s/%s/' % (base_uri, quote(path[-1], ''))
            setattr(self, path[-1], CollectionProxy(self, uri, path))

    def request(self, uri, method='GET', body=None, headers={}):
        bh = self.headers.copy()
        bh.update(headers)

        status, data = \
                self.http.request(uri, method=method, body=body, headers=bh)

        if status.get('content-type') == 'application/json':
            data = loads(data)

        if '200' == status['status']:
            return data

        if '404' == status['status']:
            if isinstance(data, dict):
                raise KeyError(data.get('message', 'not found'))
            raise KeyError('not found')

        if '400' == status['status']:
            if isinstance(data, dict):
                raise ValueError(data.get('message', 'bad request'))
            raise ValueError('bad request')

        if isinstance(data, dict):
            raise Exception(data.get('message', 'request failed'))
        raise Exception('request failed')

    @property
    def paths(self):
        if not hasattr(self, '_schema'):
            paths = self.request('%s/schema' % self.uri)['paths']
            self._paths = {tuple(k.split('/')): v \
                           for k, v in paths.iteritems()}

        return self._paths

    def iter_child_paths(self, prefix=()):
        for path, info in self.paths.iteritems():
            if len(path) == len(prefix) + 1:
                if prefix == path[:len(prefix)]:
                    yield path, info


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
