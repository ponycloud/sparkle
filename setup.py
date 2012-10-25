#!/usr/bin/python -tt

from setuptools import setup

setup(
    name = 'python-ponycloud',
    version = '1',
    author = 'The PonyCloud Team',
    description = ('distributed cloud infrastructure management'),
    license = 'MIT',
    keywords = 'cloud management',
    url = 'http://github.com/ponycloud/python-ponycloud',
    packages=['ponycloud',
              'ponycloud.twilight',
              'ponycloud.luna',
              'ponycloud.sparkle',
              'ponycloud.rainbow',
              'ponycloud.common'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: MIT License',
    ],
    scripts=['luna', 'twilight', 'sparkle']
)


# vim:set sw=4 ts=4 et:
# -*- coding: utf-8 -*-
