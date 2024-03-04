#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import unittest

from ..lib.error import OSCError


class OSCErrorTest(unittest.TestCase):
    def test_dup_error_code_exists(self):
        # Make sure all the error codes in ERR_MAPPING do not have duplicates
        err_dict = {}
        for err in OSCError.ERR_MAPPING.values():
            self.assertNotIn(err["code"], err_dict)
            err_dict[err["code"]] = err["desc"]
