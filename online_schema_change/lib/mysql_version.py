"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree. An additional grant
of patent rights can be found in the PATENTS file in the same directory.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


FB_FORK_NAME = 'fb'


class MySQLVersion(object):
    def __init__(self, version_str):
        self._version_str = version_str
        self._version = ''
        self._fork = ''
        self._build = ''
        self.parse_str()

    def parse_str(self):
        segments = self._version_str.split('-')
        self._version = segments[0]
        # It's possible for a version string to have no fork and build segment
        if len(segments) > 1:
            self._fork = segments[1]
        if len(segments) > 2:
            self._build = segments[2]

    @property
    def major(self):
        """
        Major version is the first segment of a version string
        """
        return int(self._version.split('.')[0])

    @property
    def minor(self):
        """
        Minor version is the seconds segment of a version string
        """
        return int(self._version.split('.')[1])

    @property
    def release(self):
        """
        Release is the third segment of a version string
        """
        return int(self._version.split('.')[2])

    @property
    def build(self):
        """
        If there's a build information it will be the last part the version
        string
        """
        return self._build

    @property
    def fork(self):
        """
        If the running MySQL is not a community version, it will have something
        after the general version string separated by dash
        """
        return self._fork

    @property
    def is_fb(self):
        """
        Return if current running MySQL is a Facebook fork
        """
        return self.fork == FB_FORK_NAME
