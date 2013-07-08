#!/usr/bin/python -tt

import scrypt
import os

import datetime
import time

from hmac import new as hmac
from hashlib import sha256

from functools import wraps
from flask import request, Response

import cjson

__all__ = ['check_auth', 'get_token', 'verify_token', 'requires_auth']

key = 'top_secret'


def get_token(username):
    """ Creates a new token for given username """

    # Generate salt
    salt = os.urandom(16)

    # Set validity
    valid_to = int(time.time() + 3600)

    # Assemble message
    message = ":".join([str(valid_to), username])

    # Compute HMAC of the message and secret key
    h = hmac(key, message, sha256).hexdigest()

    # scrypt hash for aditional security
    signature = scrypt.hash(h, salt, buflen=16).encode('hex').strip()
    return (message, salt.encode('hex'), signature)

def verify_token(token):
    """ Verifies if given token is valid """

    # Retrieve info from the token
    salt = token[1].decode('hex')
    validity, username = token[0].split(':', 1)
    # Verify validity
    if validity < time.time():
        return False

    # Compute HMAC...
    h = hmac(key, token[0], sha256).hexdigest()
    # ...and scrypt hash
    signature = scrypt.hash(h, salt, buflen=16).encode('hex').strip()

    return signature == token[2]


def check_auth(header, manager):
    """This function is called to check if either
    username / password combination or token is valid
    """
    try:
        type, content = header.split()
        content = content.decode('base_64')
        if type == 'Basic':
            username, password = content.split(":")
            # Verify credentials against database, scrypt.hash passwords
            try:
                user = manager.model['user'][username].desired
                if user['email'] == username \
                    and user['data']['password'] == password:
                        return username
                else:
                    # Wrong password
                    return False
            except KeyError:
                # User does not exist
                return False
        elif type == 'Token':
            token = cjson.decode(content)
            if verify_token(token):
                username = token[0].split(':', 1)[1]
                return username
            else:
                # Invalid token
                return False
    except AttributeError:
        return False

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
