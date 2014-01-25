#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Celly', 'RequestError', 'NotFoundError', 'MethodNotAllowedError',
           'BadRequestError']

from httplib2 import Http
from urllib import quote
from os.path import dirname

from simplejson import loads, dumps
import re

class RequestError(Exception):
    @classmethod
    def from_response(cls, status, data):
        code = int(status['status'])

        if not isinstance(data, dict):
            data = {}

        if code == 400:
            return BadRequestError(data.get('message', 'bad request'))

        if code == 404:
            return NotFoundError(data.get('message', 'not found'))

        if code == 405:
            return MethodNotAllowedError(data.get('message', 'method not allowed'))

        exn = cls(data.get('message', 'request failed'))
        exn.code = code
        return exn

class BadRequestError(RequestError):
    code = 400

class NotFoundError(RequestError):
    code = 404

class MethodNotAllowedError(RequestError):
    code = 405


class CollectionProxy(object):
    """Remote collection proxy."""

    def __init__(self, celly, uri, schema):
        self.celly = celly
        self.uri = uri
        self.schema = schema

    def __iter__(self):
        return iter(self.list)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.list[key]

        child_uri = '%s%s' % (self.uri, quote(key, ''))
        return EntityProxy(self.celly, child_uri, self.schema)

    def _get_key(self, item):
        if 'desired' in item:
            return item['desired'][self.schema['pkey']]
        return item['current'][self.schema['pkey']]

    @property
    def list(self):
        out = []
        for key, value in self.celly.request(self.uri).iteritems():
            child_uri = '%s%s' % (self.uri, quote(key, ''))
            out.append(EntityProxy(self.celly, child_uri, self.schema))
        return out

    def post(self, data):
        return self.celly.request(self.uri, 'POST', dumps(data))

    def patch(self, ops):
        return self.celly.request(self.uri, 'PATCH', dumps(ops))

    def __repr__(self):
        return '<CollectionProxy %s>' % self.uri


class EntityProxy(object):
    """Remote entity proxy."""

    def __init__(self, celly, uri, schema):
        self.celly = celly
        self.uri = uri
        self.schema = schema

        for name, child in self.schema['children'].iteritems():
            uri = '%s/%s/' % (self.uri, quote(name, ''))
            name = name.replace('-', '_')
            setattr(self, name, CollectionProxy(self.celly, uri, child))

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

        for name, child in self.schema['children'].iteritems():
            uri = '%s/%s/' % (base_uri, quote(name, ''))
            name = name.replace('-', '_')
            setattr(self, name, CollectionProxy(self, uri, child))

    def request(self, uri, method='GET', body=None, headers={}):
        bh = self.headers.copy()
        bh.update(headers)

        status, data = \
                self.http.request(uri, method=method, body=body, headers=bh)

        if status.get('content-type') == 'application/json':
            data = loads(data)

        if int(status['status']) in (200, 202):
            return data

        raise RequestError.from_response(status, data)


    @property
    def schema(self):
        if not hasattr(self, '_schema'):
            self._schema = {
                'children': self.request('%s/schema' % (self.uri,))
            }
        return self._schema


# vim:set sw=4 ts=4 et:
