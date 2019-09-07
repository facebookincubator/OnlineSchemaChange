#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import sys
import logging

from setuptools import setup, find_packages
import pkg_resources

install_requires = [
    "pyparsing",
    "mysqlclient",
]


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
    scripts=['osc_cli'],
    include_package_data=True,
    zip_safe=False,
    python_requires='>=3.5',
)
