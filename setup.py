#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name = 'python-ponycloud',
    version = '1',
    author = 'The PonyCloud Team',
    description = ('distributed cloud infrastructure management'),
    license = 'MIT',
    keywords = 'cloud management',
    url = 'http://github.com/ponycloud/sparkle/',
    packages=['sparkle'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: MIT License',
    ],
    scripts=['sparkle-daemon']
)


# vim:set sw=4 ts=4 et:
