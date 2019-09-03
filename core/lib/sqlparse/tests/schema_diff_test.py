#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import unittest
from ...sqlparse import parse_create, SchemaDiff


class SQLParserTest(unittest.TestCase):

    def test_two_identical_table(self):
        """
        Two identical table schema shouldn't generate any diff results
        """
        sql = (
            "Create table foo\n"
            "( column1 int )"
        )
        tbl_1 = parse_create(sql)
        tbl_2 = parse_create(sql)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertFalse(bool(tbl_diff.diffs()['removed']))
        self.assertFalse(bool(tbl_diff.diffs()['added']))

    def test_single_col_diff(self):
        sql1 = (
            "Create table foo\n"
            "( column1 int )"
        )
        sql2 = (
            "Create table foo ("
            " column1 int , "
            " column2 varchar(10))"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()['removed']), 0)
        self.assertEqual(len(tbl_diff.diffs()['added']), 1)

    def test_column_type_changed(self):
        sql1 = (
            "Create table foo "
            "( column1 int )"
        )
        sql2 = (
            "Create table foo "
            "( column1 varchar(10) )"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()['removed']), 1)
        self.assertEqual(len(tbl_diff.diffs()['added']), 1)

    def test_column_default_changed(self):
        sql1 = (
            "Create table foo "
            "( column1 int default 0)"
        )
        sql2 = (
            "Create table foo "
            "( column1 int default 1)"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()['removed']), 1)
        self.assertEqual(len(tbl_diff.diffs()['added']), 1)
        self.assertEqual(tbl_diff.diffs()['removed'][0].default, '0')
        self.assertEqual(tbl_diff.diffs()['added'][0].default, '1')

    def test_index_added(self):
        sql1 = (
            "Create table foo "
            "( column1 int default 0)"
        )
        sql2 = (
            "Create table foo( "
            "column1 int default 0,"
            "index `idx` (column1))"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()['added']), 1)

    def test_pri_key_diff(self):
        """
        Make sure adding/removing/changing PRIMARY KEY will cause a difference
        """
        sql1 = (
            "Create table foo "
            "( column1 int default 0, "
            " PRIMARY KEY (column1) )"
        )
        sql2 = (
            "Create table foo( "
            "column1 int default 0)"
        )
        sql3 = (
            "Create table foo "
            "( column1 int default 0, "
            " PRIMARY KEY (column1) comment 'abc' )"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_3 = parse_create(sql3)

        # Dropping primary key
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()['removed']), 1)

        # Adding primary key
        tbl_diff = SchemaDiff(tbl_2, tbl_1)
        self.assertEqual(len(tbl_diff.diffs()['added']), 1)

        # Chaning primary key
        tbl_diff = SchemaDiff(tbl_1, tbl_3)
        self.assertEqual(len(tbl_diff.diffs()['added']), 1)
        self.assertEqual(len(tbl_diff.diffs()['removed']), 1)

    def test_options_diff(self):
        """
        Make sure adding/removing/changing PRIMARY KEY will cause a difference
        """
        sql1 = (
            "Create table foo "
            "( column1 int default 0, "
            " PRIMARY KEY (column1) )"
        )
        tbl_1 = parse_create(sql1)
        for attr in ('charset', 'collate', 'row_format', 'key_block_size'):
            sql2 = sql1 + ' {}={} '.format(attr, '123')
            tbl_2 = parse_create(sql2)

            tbl_diff = SchemaDiff(tbl_1, tbl_2)
            self.assertEqual(len(tbl_diff.diffs()['added']), 1)

        for attr in ('comment',):
            print(attr)
            sql2 = sql1 + ' {}="{}" '.format(attr, 'abc')
            tbl_2 = parse_create(sql2)

            tbl_diff = SchemaDiff(tbl_1, tbl_2)
            self.assertEqual(len(tbl_diff.diffs()['added']), 1)

    def test_ignore_partition(self):
        """
        Make sure partition difference will be ignored if we pass in
        ignore_partition, vice versa
        """
        sql1 = (
            "Create table foo "
            "( column1 int default 0, "
            " PRIMARY KEY (column1) )"
            "   PARTITION BY RANGE(column1) "
            "  (PARTITION p0 VALUES LESS THAN (1463626800), "
            "       PARTITION p1 VALUES LESS THAN (1464049800), "
            "       PARTITION p2 VALUES LESS THAN (1464472800)) "
        )
        sql2 = (
            "Create table foo "
            "( column1 int default 0, "
            " PRIMARY KEY (column1) )"
            "   PARTITION BY RANGE(column1) "
            "  (PARTITION p0 VALUES LESS THAN (1463626800), "
            "       PARTITION p1 VALUES LESS THAN (1464049800)) "
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)

        tbl_diff = SchemaDiff(tbl_1, tbl_2, ignore_partition=True)
        self.assertEqual(len(tbl_diff.diffs()['added']), 0)
        self.assertEqual(len(tbl_diff.diffs()['removed']), 0)

        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()['added']), 1)
        self.assertEqual(len(tbl_diff.diffs()['removed']), 1)
