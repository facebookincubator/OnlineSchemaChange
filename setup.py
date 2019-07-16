"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import logging

from setuptools import setup, find_packages
import pkg_resources

install_requires = [
    "six",
    "pyparsing",
    "mysqlclient",
]


extras_require = {
    ':python_version < "3.0"': ['subprocess32']
}

try:
    if 'bdist_wheel' not in sys.argv:
        for key, value in extras_require.items():
            if key.startswith(':') and pkg_resources.evaluate_marker(key[1:]):
                install_requires.extend(value)
except Exception:
    logging.getLogger(__name__).exception(
        'Something went wrong calculating platform specific dependencies, so '
        "you're getting them all!"
    )
    for key, value in extras_require.items():
        if key.startswith(':'):
            install_requires.extend(value)

setup(
    name='osc',
    version='0.0.1',
    packages=find_packages(),
    author="Luke Lu",
    author_email="junyilu@fb.com",
    url='https://github.com/facebookincubator/OnlineSchemaChange',
    description='Online Schema Change for MySQL',
    long_description=open('README.rst').read(),
    install_requires=install_requires,
    extras_require=extras_require,
    scripts=['osc_cli'],
    include_package_data=True,
    zip_safe=False,
)
