#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import unittest

from ..lib.mysql_version import MySQLVersion


class MySQLVersionTest(unittest.TestCase):
    def test_fb_fork(self):
        """
        Tests for a Facebook log build
        """
        v_str = "5.1.2-fb-log"
        v_obj = MySQLVersion(v_str)
        self.assertTrue(v_obj.is_fb)
        self.assertTrue(v_obj.major == 5)
        self.assertTrue(v_obj.minor == 1)
        self.assertTrue(v_obj.release == 2)
        self.assertTrue(v_obj.build == "log")

    def test_community_no_build(self):
        """
        Tests for a community version
        """
        v_str = "5.6.30"
        v_obj = MySQLVersion(v_str)
        self.assertFalse(v_obj.is_fb)
        self.assertTrue(v_obj.major == 5)
        self.assertTrue(v_obj.minor == 6)
        self.assertTrue(v_obj.release == 30)
        self.assertTrue(v_obj.build == "")

    def test_community_debug_build(self):
        """
        Tests for a community version with log enabled
        """
        v_str = "5.6.30-log"
        v_obj = MySQLVersion(v_str)
        self.assertFalse(v_obj.is_fb)
        self.assertTrue(v_obj.major == 5)
        self.assertTrue(v_obj.minor == 6)
        self.assertTrue(v_obj.release == 30)

    def test_lt_gt_major_difference(self):
        left = "5.6.30-log"
        right = "8.0.1-log"
        v_left = MySQLVersion(left)
        v_right = MySQLVersion(right)
        self.assertTrue(v_left < v_right)
        self.assertTrue(v_left <= v_right)
        self.assertTrue(v_right > v_left)
        self.assertTrue(v_right >= v_left)

    def test_lt_gt_minor_difference(self):
        left = "5.6.30-log"
        right = "5.7.1-log"
        v_left = MySQLVersion(left)
        v_right = MySQLVersion(right)
        self.assertTrue(v_left < v_right)
        self.assertTrue(v_left <= v_right)
        self.assertTrue(v_right > v_left)
        self.assertTrue(v_right >= v_left)

    def test_lt_gt_release_difference(self):
        left = "5.6.30-log"
        right = "5.6.50-log"
        v_left = MySQLVersion(left)
        v_right = MySQLVersion(right)
        self.assertTrue(v_left < v_right)
        self.assertTrue(v_left <= v_right)
        self.assertTrue(v_right > v_left)
        self.assertTrue(v_right >= v_left)

    def test_le_ge(self):
        left = "5.6.30-log"
        right = "5.6.30-log"
        v_left = MySQLVersion(left)
        v_right = MySQLVersion(right)
        self.assertTrue(v_left <= v_right)
        self.assertTrue(v_right >= v_left)
