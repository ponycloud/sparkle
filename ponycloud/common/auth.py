#!/usr/bin/python -tt

import scrypt
import os

import datetime
import time

from hmac import new as hmac
from hashlib import sha256

import base64

from functools import wraps
from flask import request, Response

import cjson

__all__ = ['check_auth', 'get_token', 'verify_token', 'requires_auth']

def get_token(username, key, validity=3600):
    """ Creates a new token for given username """

    # Generate salt
    salt = os.urandom(16)

    # Set validity
    valid_to = int(time.time() + validity)

    # Assemble message
    message = ":".join([str(valid_to), username])

    # Compute temporary key specific to this message.
    tmp_key = hmac(key, salt, sha256).digest()

    # Compute HMAC of the message and secret key
    signature = hmac(tmp_key, message, sha256).hexdigest()

    return (message, salt.encode('hex'), signature)

def verify_token(token, key):
    """ Verifies if given token is valid """

    # Retrieve info from the token
    salt = token[1].decode('hex')
    validity, username = token[0].split(':', 1)
    # Verify validity
    if int(validity) < int(time.time()):
        return False
    # Compute HMAC...
    tmp_key = hmac(key, salt, sha256).digest()

    signature = hmac(tmp_key, token[0], sha256).hexdigest()

    return signature == token[2]


def check_auth(header, manager):
    """This function is called to check if either
    username / password combination or token is valid
    """
    try:
        type, content = header.split()
        content = base64.b64decode(content)
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
            except (TypeError, KeyError):
                # User does not exist or something strange happened
                return False
        elif type == 'Token':
            try:
                token = cjson.decode(content)
                if verify_token(token, manager.authkeys['passkey']):
                    username = token[0].split(':', 1)[1]
                    return username
                else:
                    # Invalid token
                    return False
            except:
                return False
    except AttributeError:
        return False

# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
