#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

__all__ = ['UserError', 'DataError', 'AccessError', 'PathError',
           'ConflictError']

class UserError(Exception):
    """
    Common parent for API exceptions.
    Should map to Bad Request reply to API client.
    """

    name = 'user-error'
    status = 400

    def __init__(self, message, path=None):
        """
        Store details about the failure.

        :param message:  Description of the failure.
        :param path:      String or list path to the dbdict node the failure
                          is related to.  Converted to path string.
        """

        assert isinstance(message, basestring), \
               'message must be a string'

        super(UserError, self).__init__(message)

        assert path is None \
            or isinstance(path, basestring) \
            or isinstance(path, list), \
               'path must be None, a string or list of strings'

        if isinstance(path, list):
            path = [unicode(p) for p in path]
            path = [p.replace('~', '~0') for p in path]
            path = [p.replace('/', '~1') for p in path]
            self.path = '/' + '/'.join(path)
        else:
            self.path = path

    @property
    def json(self):
        info = {
            'error': self.name,
            'message': self.message,
        }

        if self.path:
            info['path'] = self.path

        return info


class AccessError(UserError):
    """
    Failure that has to do with authorization.
    Should map to Forbidden reply to API client.
    """

    name = 'access-denied'
    status = 403


class DataError(UserError):
    """
    Failure that has to do with data in the model.
    Should map to Bad Request reply to API client.
    """

    name = 'invalid-data'
    status = 400


class ConflictError(DataError):
    """
    Special case for PATCH test operation failure.
    Should map to a Conflict reply to API client.
    """

    name = 'conflict'
    status = 409


class PathError(DataError):
    """
    Requested piece of data have not been found.
    Should map to Not Found reply to API client.
    """

    status = 404


# vim:set sw=4 ts=4 et:
