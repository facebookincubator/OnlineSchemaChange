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
from ..lib.payload.direct import DirectPayload
from ..lib.error import OSCError
from ..lib import db as db_lib
from mock import MagicMock
import MySQLdb


class BasePayloadTestCase(unittest.TestCase):
    def setUp(self):
        self._payload.ddl_file_list = ['ddl_file1.sql', 'ddl_file2.sql']


class DirectPayloadDeadMySQL(BasePayloadTestCase):
    def setUp(self):
        db_lib.MySQLSocketConnection = MagicMock(
            side_effect=MySQLdb.OperationalError(2013, 'mock unconnectable'))
        self._payload = DirectPayload()
        self._payload.read_ddl_files = MagicMock()
        super(DirectPayloadDeadMySQL, self).setUp()

    def test_run_with_dead_mysql(self):
        with self.assertRaises(OSCError) as e:
            self._payload.init_conn()
            self._payload.run()
            oscerr = e.exception
            self.assertEqual(oscerr.err_key, 'GENERIC_MYSQL_ERRO')


class DirectPayloadSQLFailed(BasePayloadTestCase):
    def setUp(self):
        db_lib.MySQLSocketConnection.query = MagicMock(
            side_effect=MySQLdb.OperationalError(2013, 'transaction failed'))
        self._payload = DirectPayload()
        self._payload.read_ddl_files = MagicMock()
        super(DirectPayloadSQLFailed, self).setUp()

    def test_run_with_failed_sql(self):
        with self.assertRaises(OSCError) as e:
            self._payload.init_conn()
            self._payload.run()
        oscerr = e.exception
        self.assertEqual(oscerr.err_key, 'GENERIC_MYSQL_ERROR')
