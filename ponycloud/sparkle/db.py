#!/usr/bin/python -tt

__all__ = ['select_dict', 'select_all_dicts', 'insert_dict', 'update_dict']

import sqlalchemy
import sqlsoup
import psycopg2
import psycopg2.extras
import decimal

from ponycloud.common.util import uuidgen

psycopg2.extras.register_inet()
psycopg2.extensions.register_type(psycopg2.extensions.new_array_type((1041,), "INETARRAY", psycopg2.extensions.INET))
psycopg2.extensions.register_type(psycopg2.extensions.INETARRAY)

def select_dict(entity, **primary_keys):
    """
    Selectes entity from database as a dictionary.

    entity -- sqlsoup entity
    primary_keys -- keys to filter entity by
    """
    return soup_2_dict_type(soup2dict(entity.filter_by(**primary_keys).one()))

def select_all_dicts(entity, **filters):
    """
    Selects all instances of specified entity as a list of dictionaries.

    entity -- sqlsoup entity
    filters -- key/value pairs to filter entities by
    """
    return [soup_2_dict_type(soup2dict(en))
            for en in entity.filter_by(**filters).all()]

def soup2dict(obj):
    """
    Converts sqlsoup entity (+ related entities) to dictionary.

    obj -- SQLsoup entity (with relations) to encode
    visited -- list of already visitied related elements (internal use)
    """

    if hasattr(obj, '_table'):
        fields = {}

        for field in dir(obj):
            if field.startswith('_') or field == 'c':
                # Ignore private properties.
                continue

            fields[field] = soup2dict(getattr(obj, field))

        return fields

    elif isinstance(obj, list):
        return [soup2dict(list_item) for list_item in obj]

    else:
        return obj


def find_on_stack(stack, key):
    for k, v in stack:
        if key == k:
            return v


def insert_dict(db, entity_name, obj, added=None):
    """
    Inserts dicionary into database. Every related entity is parsed and inserted separately.

    db -- sqlsoup database
    entity_name -- name (equals table name) of top level entity
    obj -- dictionary to insert
    added -- stack of generated ids of new entities (internal use)
    """

    if added is None:
        added = []

    entity = getattr(db, entity_name)

    # generate primary key(s) for current entity
    for column in entity._table.columns:
        if column.foreign_keys:
            # User have actually filled in something for us! Yay!
            if column.name in obj:
                continue

            # Maybe the other side already has a value.
            for fk in column.foreign_keys:
                referenced_table = find_on_stack(added, fk.column.table.name)
                if referenced_table is None:
                    continue
                if fk.column.name not in referenced_table:
                    continue
                obj[column.name] = referenced_table[fk.column.name]
                break

        elif column.primary_key:
            # Only generate for UUID type - others are left empty and handled by database functions
            if str(column.type) == 'UUID':
                obj[column.name] = uuidgen()

    added.append((entity_name, obj))

    # Recurse into related columns.
    for key in obj:
        column = getattr(entity, key)
        if hasattr(column.property, 'target'):
            for item in obj[key]:
                insert_dict(item, column.property.target.name, added)

    added.pop()

    for key in obj.keys():
        column = getattr(entity, key)
        if hasattr(column.property, 'target'):
            del obj[key]
        else:
            # convert special data types from text to proper object type
            obj[key] = dict_2_soup_type(column.property.columns[0].type, obj[key])

    entity.insert(**obj)

def dict_2_soup_type(db_type, value):
    type_name = str(db_type)

    if isinstance(value, list) :
        return [dict_2_soup_type(db_type, x) for x in value]

    if type_name[:-2] == 'INET':
        return psycopg2.extras.Inet(value)

    return value

def update_dict(entity, obj, **primary_keys):
    """
    Updates specified database entity from a dict.

    entity -- entity the dict represents
    obj -- dictionary containing new data
    primary_keys -- keyword arguments with values of primary key(s), which will be updated
    """

    for key in obj.keys():
        column = getattr(entity, key)
        if hasattr(column.property, 'target') or column.property.columns[0].primary_key:
            del obj[key]
        else:
            obj[key] = dict_2_soup_type(column.property.columns[0].type, obj[key])

    entity.filter_by(**primary_keys).update(obj)


def soup_2_dict_type(data):
    if isinstance(data, dict):
        return dict([(k, soup_2_dict_type(v)) for k, v in data.items()])

    if isinstance(data, list):
        return [soup_2_dict_type(x) for x in data]

    if isinstance(data, psycopg2.extras.Inet):
        return data.addr

    if isinstance(data, decimal.Decimal):
        return str(data)

    return data


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
