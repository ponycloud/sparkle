#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

__all__ = ['split_list', 'uuidgen']

from uuid import uuid4

def split_list(self, sep, maxsplit=None):
    """
    Splits list using separator element.

    This is identical to string splitting, only on lists.
    """

    # Always return at least one part.
    parts = [[]]

    # Start with zero (obviously) splits done.
    splits = 0

    # Iterate over all input items.
    for item in self:
        if maxsplit is not None and splits >= maxsplit:
            # After reaching maximum number of splits,
            # just append items to the last part in the result.
            parts[-1].append(item)
            continue

        if item == sep:
            # Output new part with every separator.
            parts.append([])
            splits += 1
        else:
            # Otherwise just append the item to the last part.
            parts[-1].append(item)

    return parts


def uuidgen():
    """
    Generates random UUID string.
    """
    return str(uuid4())


# vim:set sw=4 ts=4 et:
