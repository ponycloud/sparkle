#!/usr/bin/python -tt

__all__ = ['Flaskful', 'json_response']

from flask import Flask, Response, make_response, request

from traceback import print_exc

from auth import check_auth

from functools import wraps
from simplejson import dumps

def json_response(data, status=200):
    """
    Creates JSON response object from given structure.
    """
    if isinstance(data, Response):
        return data
    else:
        resp = make_response(dumps(data, indent=2) + '\n', status)
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
            }, 400)

        @self.errorhandler(404)
        def page_not_found(e):
            return json_response({
                'error': 'not-found',
                'message': e.description \
                                if not e.description.startswith('<p>') \
                                else 'requested url was not found',
            }, 404)

        @self.errorhandler(500)
        def internal_server_error(e):
            return json_response({
                'error': 'internal-server-error',
                'hint': unicode(e),
            }, 500)

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

    def authenticate(self):
        """
        Sends a 401 response that enables basic auth
        """
        return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

    def requires_auth(self, manager):
        def wrap(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                username = check_auth(request.headers.get("Authorization"), manager)
                if username is False:
                    return self.authenticate()
                return f(username, *args, **kwargs)
            return wrapper
        return wrap



# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
