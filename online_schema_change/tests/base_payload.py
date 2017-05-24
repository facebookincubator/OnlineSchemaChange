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
from ..lib.payload.base import Payload
from ..lib.error import OSCError
from mock import Mock


class BasePayloadTestCase(unittest.TestCase):
    def test_failed_to_get_name_lock(self):
        payload = Payload()
        payload.skip_named_lock = False
        with self.assertRaises(OSCError) as err_context:
            payload.query = Mock(return_value=None)
            payload.get_osc_lock()
        self.assertEqual(err_context.exception.err_key, 'UNABLE_TO_GET_LOCK')

        with self.assertRaises(OSCError) as err_context:
            payload.query = Mock(return_value=[{'lockstatus': 0}])
            payload.get_osc_lock()
        self.assertEqual(err_context.exception.err_key, 'UNABLE_TO_GET_LOCK')

    def test_successfully_get_name_lock(self):
        payload = Payload()
        payload.skip_named_lock = False
        payload.query = Mock(return_value=[{'lockstatus': 1}])
        payload.get_osc_lock()

    def test_get_name_lock_ignore_failure(self):
        payload = Payload()
        payload.skip_named_lock = True
        # Nothing should happen if we skip named lock
        payload.query = Mock(return_value=None)
        self.assertFalse(payload.query.called)
