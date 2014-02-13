#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

__doc__ = """
Validator Functions

Routines that help with validation of various data structures the API
needs to make sense of.
"""

__all__ = ['validate_json_patch', 'ValidationError']

from sparkle.schema import schema

from jsonschema import validate as schema_validate, ValidationError

import os.path
import yaml


# Load the JSON Patch schema specification from an outside file.
with open(os.path.dirname(__file__) + '/patch.schema.yaml') as fp:
    JSON_PATCH_SCHEMA = yaml.load(fp)


def validate_json_patch(data):
    """
    Validate that given object corresponds to a valid JSON Patch.

    We tolerate usage of list instead of string for path properties,
    accept an additional operation 'x-merge' but otherwise don't peek
    into the patch values.

    Raises an ValidationError if the patch does not match the
    prescribed schema.  See jsonschema.ValidationError for details.
    """

    jsonschema.validate(data, schema)


# vim:set sw=4 ts=4 et:
