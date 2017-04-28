"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree. An additional grant
of patent rights can be found in the PATENTS file in the same directory.
"""

import sys
from setuptools import setup, find_packages

required_pkgs = [
    "six",
    "pyparsing",
    "mysqlclient",
]
if sys.version_info[0] == 2:  # PY2
    required_pkgs.append("subprocess32")

setup(
    name='osc',
    version='1.0.0',
    packages=find_packages(),
    url='https://github.com/facebookincubator/OnlineSchemaChange',
    description='Online Schema Change for MySQL',
    long_description=open('README.md').read(),
    install_requires=required_pkgs,
    dependency_links=[
        "http://github.com/PyMySQL/mysqlclient-python/tarball/master",
        "http://github.com/google/python-subprocess32/tarball/master",
    ],
    scripts=['osc_cli'],
    include_package_data=True,
    zip_safe=False,
)
