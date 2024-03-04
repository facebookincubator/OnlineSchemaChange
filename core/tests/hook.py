#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import unittest
from unittest.mock import Mock, patch

from ..lib.error import OSCError
from ..lib.hook import SQLHook


class SQLHookTest(unittest.TestCase):
    def gen_sql_hook_obj(self, expected, got):
        sql_obj = SQLHook()
        sql_obj._is_select = True
        sql_obj._dbh = Mock()
        sql_obj._sqls = ["select 1"]
        expected_lines = []
        for row in expected:
            expected_lines.append("\t".join([str(col) for col in row]))
        sql_obj._expected_lines = expected_lines
        sql_obj._dbh.query_array = Mock(return_value=got)
        return sql_obj

    def test_consitent_data_not_raise_exception(self):
        with patch.object(SQLHook, "read_sqls"):
            sql_obj = self.gen_sql_hook_obj(expected=((1, 1, 1),), got=((1, 1, 1),))
            sql_obj.execute_sqls()

    def test_expect_data_mismatch(self):
        with patch.object(SQLHook, "read_sqls"):
            sql_obj = self.gen_sql_hook_obj(expected=((1, 1, 1),), got=((1, 1, 2),))
            with self.assertRaises(OSCError) as context:
                sql_obj.execute_sqls()
            self.assertEqual(context.exception.err_key, "ASSERTION_ERROR")

    def test_expect_more_rows(self):
        with patch.object(SQLHook, "read_sqls"):
            sql_obj = self.gen_sql_hook_obj(
                expected=((1, 1, 1), (2, 2, 2)), got=((1, 1, 1),)
            )
            with self.assertRaises(OSCError) as context:
                sql_obj.execute_sqls()
            self.assertEqual(context.exception.err_key, "ASSERTION_ERROR")

    def test_expect_less_rows(self):
        with patch.object(SQLHook, "read_sqls"):
            sql_obj = self.gen_sql_hook_obj(
                expected=((1, 1, 1),), got=((1, 1, 1), (2, 2, 2))
            )
            with self.assertRaises(OSCError) as context:
                sql_obj.execute_sqls()
            self.assertEqual(context.exception.err_key, "ASSERTION_ERROR")

    def test_expect_different_order(self):
        with patch.object(SQLHook, "read_sqls"):
            sql_obj = self.gen_sql_hook_obj(
                expected=((2, 2, 2), (1, 1, 1)), got=((1, 1, 1), (2, 2, 2))
            )
            with self.assertRaises(OSCError) as context:
                sql_obj.execute_sqls()
            self.assertEqual(context.exception.err_key, "ASSERTION_ERROR")
