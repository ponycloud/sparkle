#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['Flaskful', 'json_response']

from flask import Flask, Response, make_response, request
from werkzeug.exceptions import Unauthorized
from functools import wraps
from simplejson import dumps
from traceback import print_exc
from auth import authenticate

def json_response(data, status=200, headers={}):
    """
    Creates JSON response object from given structure.
    """
    if isinstance(data, Response):
        return data
    else:
        resp = make_response(dumps(data, indent=2) + '\n', status)
        resp.headers = headers

        if status == 401:
            resp.headers['WWW-Authenticate'] = 'Basic realm="Sparkle"'

        resp.headers['content-type'] = 'application/json'
        return resp


class Flaskful(Flask):
    """
    A bit extended Flask to accomodate for RESTful applications.
    """

    def __init__(self, *args, **kwargs):
        """
        Performs REST API initialization.
        """
        super(Flaskful, self).__init__(*args, **kwargs)

        @self.errorhandler(400)
        def bad_request(e):
            return json_response({
                'error': 'bad-request',
                'message': e.description.strip(),
            }, 400, getattr(e, 'headers', {}))

        @self.errorhandler(401)
        def unauthorized(e):
            return json_response({
                'error': 'unauthorized',
                'message': e.description \
                            if not e.description.startswith('<p>') \
                            else 'you need to login to access this url'
            }, 401, getattr(e, 'headers', {}))

        @self.errorhandler(404)
        def page_not_found(e):
            return json_response({
                'error': 'not-found',
                'message': e.description \
                                if not e.description.startswith('<p>') \
                                else 'requested url was not found',
            }, 404, getattr(e, 'headers', {}))

        @self.errorhandler(405)
        def method_not_allowed(e):
            return json_response({
                'error': 'method-not-allowed',
                'message': e.description \
                            if not e.description.startswith('<p>') \
                            else 'method not allowed',
            }, 405, getattr(e, 'headers', {}))

        @self.errorhandler(500)
        def internal_server_error(e):
            return json_response({
                'error': 'internal-server-error',
                'hint': unicode(e),
            }, 500, getattr(e, 'headers', {}))

    def route_json(self, rule, **options):
        """
        Same as the classical `route`, but converts result to JSON string.
        """

        def wrap(fn):
            @self.route(rule, **options)
            @wraps(fn)
            def wrapper(*args, **kwargs):
                return json_response(fn(*args, **kwargs))
            return wrapper
        return wrap

    def require_credentials(self, manager):
        def wrap(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                auth = request.headers.get('Authorization')
                if not auth:
                    raise Unauthorized('no credentials supplied')

                credentials = authenticate(auth, manager)
                if not credentials:
                    raise Unauthorized('invalid credentials supplied')

                kwargs['credentials'] = credentials

                return f(*args, **kwargs)
            return wrapper
        return wrap


# vim:set sw=4 ts=4 et:
