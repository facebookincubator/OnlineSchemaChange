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
    PROD_VERSION = "8.0.32-202407011440.prod"
    DEV_VERSION = "8.0.32-202407051440.dev.alexbud"

    def test_major(self):
        self.assertEqual(MySQLVersion(self.PROD_VERSION).major, 8)
        self.assertEqual(MySQLVersion(self.DEV_VERSION).major, 8)

    def test_minor(self):
        self.assertEqual(MySQLVersion(self.PROD_VERSION).minor, 0)
        self.assertEqual(MySQLVersion(self.DEV_VERSION).minor, 0)

    def test_release(self):
        self.assertEqual(MySQLVersion(self.PROD_VERSION).release, 32)
        self.assertEqual(MySQLVersion(self.DEV_VERSION).release, 32)

    def test_build(self):
        self.assertEqual(MySQLVersion(self.PROD_VERSION).build, "202407011440")
        self.assertEqual(MySQLVersion(self.DEV_VERSION).build, "202407051440")

    def test_eq(self):
        self.assertEqual(
            MySQLVersion(self.PROD_VERSION), MySQLVersion(self.PROD_VERSION)
        )
        self.assertEqual(MySQLVersion(self.DEV_VERSION), MySQLVersion(self.DEV_VERSION))

    def test_lt_gt_major_difference(self):
        left = MySQLVersion("5.6.1-202407011440.prod")
        right = MySQLVersion("8.0.32-202407011440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

    def test_lt_gt_minor_difference(self):
        left = MySQLVersion("5.6.10-202407011440.prod")
        right = MySQLVersion("5.7.2-202407011440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

    def test_lt_gt_release_difference(self):
        left = MySQLVersion("5.6.1-202407011440.prod")
        right = MySQLVersion("5.6.10-202407011440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

    def test_lt_gt_build_difference(self):
        # Year
        left = MySQLVersion("5.6.1-202307011440.prod")
        right = MySQLVersion("5.6.1-202407011440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

        # Month
        left = MySQLVersion("5.6.1-202407011440.prod")
        right = MySQLVersion("5.6.1-202408011440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

        # Day
        left = MySQLVersion("5.6.1-202407011440.prod")
        right = MySQLVersion("5.6.1-202407051440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

        # Hour
        left = MySQLVersion("5.6.1-202407011340.prod")
        right = MySQLVersion("5.6.1-202407011440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

        # Minute
        left = MySQLVersion("5.6.1-202407011439.prod")
        right = MySQLVersion("5.6.1-202407011440.prod")
        self.assertTrue(left < right)
        self.assertTrue(left <= right)
        self.assertTrue(right > left)
        self.assertTrue(right >= left)

    def test_le_ge(self):
        v = MySQLVersion("5.7.2-202407011440.prod")
        self.assertTrue(v <= v)
        self.assertTrue(v >= v)
