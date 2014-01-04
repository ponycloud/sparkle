#!/usr/bin/python -tt

import scrypt
import os

from simplejson import loads
from hmac import new as hmac
from hashlib import sha256
from time import time

from functools import wraps
from flask import request, Response

__all__ = ['sign_token', 'extract_token']


def encode(datum):
    return datum.encode('base64').replace('\n', '')


def decode(datum):
    return datum.decode('base64')


def sign_token(payload, key, validity=3600):
    """
    Wrap token payload in an envelope with a verifiable signature.

    :param payload:   Binary string to sign.
    :param key:       Persistent server key.
    :param validity:  Number of seconds for token to remain valid.
    :returns: String to be passed to extract_token().
    """

    assert isinstance(payload, str),  'payload must be a binary string'
    assert isinstance(key, str),      'key must be a binary string'
    assert isinstance(validity, int), 'validity must be an integer'

    # Define expiration time to mitigate replay attack.
    valid_until = str(int(time() + validity))

    # Serialize the payload along with expiration timestamp for signing.
    message = ':'.join((valid_until, payload))

    # Derive a temporary signing key to mitigate volume attack.
    salt = os.urandom(16)
    tmp_key = hmac(key, salt, sha256).digest()

    # Sign the message with the temporary key.
    signature = hmac(tmp_key, message, sha256).digest()

    # Create the final string in the form 'signature:salt:timestamp:payload',
    # every individual component being base64-encoded.
    return ':'.join(map(encode, (signature, salt, valid_until, payload)))


def passcmp(a, b):
    """Length-only-dependent string comparison function."""

    if len(a) != len(b):
        return False

    diff = 0

    for xa, xb in zip(str(a), str(b)):
        diff += abs(ord(xa) - ord(xb))

    return 0 == diff


def extract_token(token, key):
    """
    Extract and return token payload if valid or raise ValueError if the
    token have been tampered with or have expired since being issued.

    :param token:  String to extract payload from.
    :param key:    Persistent server key.
    :returns: Extracted token payload string.
    """

    assert isinstance(token, basestring), 'token must be a string'
    assert isinstance(key, basestring),   'key must be a string'

    signature, salt, timestamp, payload = map(decode, token.split(':', 4))

    message = ':'.join((timestamp, payload))
    tmp_key = hmac(key, salt, sha256).digest()

    if not passcmp(hmac(tmp_key, message, sha256).digest(), signature):
        raise ValueError('token have been tampered with')

    if int(timestamp) < int(time()):
        raise ValueError('token have expired')

    return payload


def authenticate(header, manager):
    """
    Validate Basic or Token authentication data and return payload.

    The resulting payload will be either ``{'tenant': 'tenant-uuid'}``,
    ``{'user': 'address'}``, or ``{}`` depending on the authentication
    payload and password validity.
    """

    try:
        kind, data = header.split()

        if kind == 'Basic':
            email, password = data.decode('base64').split(':')

            auth_data = manager.model['user'][email].desired['data']

            if auth_data.get('method') is None:
                if auth_data['password'] == password:
                    return {'user': email}

            else:
                print 'unsupported authentication data %r' % auth_data

            return {}

        if kind == 'Token':
                apikey = manager.authkeys['apikey']
                return loads(extract_token(data, apikey))

    except (TypeError, KeyError, ValueError):
        return {}


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
