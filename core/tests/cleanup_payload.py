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

import unittest
from ..lib.payload.cleanup import CleanupPayload


class CleanupPayloadTestCase(unittest.TestCase):
    def test_remove_all_file_entries(self):
        payload = CleanupPayload()
        payload.add_file_entry('/this/is/a/path/')
        payload.add_file_entry('/this/is/another/path/')
        payload.remove_all_file_entries()
        self.assertEqual(len(payload.files_to_clean), 0)

    def test_add_file_entry(self):
        payload = CleanupPayload()
        path = '/this/is/a/path/'
        payload.add_file_entry(path)
        self.assertEqual(payload.files_to_clean, [path])
