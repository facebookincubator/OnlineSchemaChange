#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import unittest
from unittest.mock import MagicMock

import MySQLdb

from ..lib import db as db_lib
from ..lib.error import OSCError
from ..lib.payload.direct import DirectPayload


class BasePayloadTestCase(unittest.TestCase):
    def setUp(self):
        self._payload.ddl_file_list = ["ddl_file1.sql", "ddl_file2.sql"]


class DirectPayloadDeadMySQL(BasePayloadTestCase):
    def setUp(self):
        db_lib.MySQLSocketConnection = MagicMock(
            side_effect=MySQLdb.OperationalError(2013, "mock unconnectable")
        )
        self._payload = DirectPayload()
        self._payload.read_ddl_files = MagicMock()
        super(DirectPayloadDeadMySQL, self).setUp()

    def test_run_with_dead_mysql(self):
        with self.assertRaises(OSCError) as e:
            self._payload.init_conn()
            self._payload.run()

        self.assertEqual(e.exception.err_key, "GENERIC_MYSQL_ERROR")


class DirectPayloadSQLFailed(BasePayloadTestCase):
    def setUp(self):
        db_lib.MySQLSocketConnection.query = MagicMock(
            side_effect=MySQLdb.OperationalError(2013, "transaction failed")
        )
        self._payload = DirectPayload()
        self._payload.read_ddl_files = MagicMock()
        super(DirectPayloadSQLFailed, self).setUp()

    def test_run_with_failed_sql(self):
        with self.assertRaises(OSCError) as e:
            self._payload.init_conn()
            self._payload.run()
        oscerr = e.exception
        self.assertEqual(oscerr.err_key, "GENERIC_MYSQL_ERROR")
