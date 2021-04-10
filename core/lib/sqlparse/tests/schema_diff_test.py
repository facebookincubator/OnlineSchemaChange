#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import unittest
from ...sqlparse import (
    parse_create,
    SchemaDiff,
    get_type_conv_columns,
    need_default_ts_bootstrap,
)


class SQLParserTest(unittest.TestCase):
    def test_two_identical_table(self):
        """
        Two identical table schema shouldn't generate any diff results
        """
        sql = "Create table foo\n" "( column1 int )"
        tbl_1 = parse_create(sql)
        tbl_2 = parse_create(sql)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertFalse(bool(tbl_diff.diffs()["removed"]))
        self.assertFalse(bool(tbl_diff.diffs()["added"]))

    def test_single_col_diff(self):
        sql1 = "Create table foo\n" "( column1 int )"
        sql2 = "Create table foo (" " column1 int , " " column2 varchar(10))"
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 0)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

    def test_col_order(self):
        sql1 = (
            "Create table foo ("
            " column2 varchar(10) ,"
            " column1 int ,"
            " column3 int)"
        )
        sql2 = (
            "Create table foo ("
            " column1 int , "
            " column2 varchar(10) ,"
            " column3 int)"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["msgs"]), 3)

    def test_col_order_with_new_col(self):
        sql1 = "Create table foo (" " column2 varchar(10) ," " column1 int)"
        sql2 = (
            "Create table foo ("
            " column1 int , "
            " column2 varchar(10) ,"
            " column3 int)"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["msgs"]), 3)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

    def test_column_type_changed(self):
        sql1 = "Create table foo " "( column1 int )"
        sql2 = "Create table foo " "( column1 varchar(10) )"
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 1)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

    def test_column_default_changed(self):
        sql1 = "Create table foo " "( column1 int default 0)"
        sql2 = "Create table foo " "( column1 int default 1)"
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 1)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)
        self.assertEqual(tbl_diff.diffs()["removed"][0].default, "0")
        self.assertEqual(tbl_diff.diffs()["added"][0].default, "1")

    def test_index_added(self):
        sql1 = "Create table foo " "( column1 int default 0)"
        sql2 = "Create table foo( " "column1 int default 0," "index `idx` (column1))"
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

    def test_pri_key_diff(self):
        """
        Make sure adding/removing/changing PRIMARY KEY will cause a difference
        """
        sql1 = (
            "Create table foo " "( column1 int default 0, " " PRIMARY KEY (column1) )"
        )
        sql2 = "Create table foo( " "column1 int default 0)"
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
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 1)

        # Adding primary key
        tbl_diff = SchemaDiff(tbl_2, tbl_1)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

        # Chaning primary key
        tbl_diff = SchemaDiff(tbl_1, tbl_3)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 1)

    def test_options_diff(self):
        """
        Make sure adding/removing/changing PRIMARY KEY will cause a difference
        """
        sql1 = (
            "Create table foo " "( column1 int default 0, " " PRIMARY KEY (column1) )"
        )
        tbl_1 = parse_create(sql1)
        for attr in ("charset", "collate", "row_format", "key_block_size"):
            sql2 = sql1 + " {}={} ".format(attr, "123")
            tbl_2 = parse_create(sql2)

            tbl_diff = SchemaDiff(tbl_1, tbl_2)
            self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

        for attr in ("comment",):
            print(attr)
            sql2 = sql1 + ' {}="{}" '.format(attr, "abc")
            tbl_2 = parse_create(sql2)

            tbl_diff = SchemaDiff(tbl_1, tbl_2)
            self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

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
        self.assertEqual(len(tbl_diff.diffs()["added"]), 0)
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 0)

        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 1)

    def test_type_conv_columns(self):
        sql1 = (
            "Create table foo ("
            "column1 int default 0, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ")"
        )
        sql2 = (
            "Create table foo ("
            "column1 int default 0, "
            "column2 varchar(20) default '', "
            "column3 bigint default 0 "
            ")"
        )
        tbl_1 = parse_create(sql1)
        tbl_2 = parse_create(sql2)
        type_conv_columns = get_type_conv_columns(tbl_1, tbl_2)

        # Both column type and length change is considered as type conversion
        self.assertEqual(len(type_conv_columns), 2)
        self.assertEqual(type_conv_columns[0].name, "column2")
        self.assertEqual(type_conv_columns[1].name, "column3")

    def test_meta_diff_with_commas(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ") charset=utf8 engine=INNODB"
        )

        # Schema identical but comment added in table attrs
        # The commas in the table attrs should be a NOP for parsing
        sql2 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ") engine=INNODB , charset=utf8, comment='whatever'"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertNotEqual(obj1, obj2)

        diff_obj = SchemaDiff(obj1, obj2)
        diffs = diff_obj.diffs()
        self.assertEqual(diffs["attrs_modified"], ["comment"])


class HelpersTest(unittest.TestCase):
    def test_need_default_ts_bootstrap(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ") charset=utf8 engine=INNODB"
        )

        sql2 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 timestamp default CURRENT_TIMESTAMP "
            ") charset=utf8 engine=INNODB"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_add_irrelevant_col(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ") charset=utf8 engine=INNODB"
        )

        sql2 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 date "
            ") charset=utf8 engine=INNODB"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertFalse(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_implicit_ts_default(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ") charset=utf8 engine=INNODB"
        )

        sql2 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 timestamp "
            ") charset=utf8 engine=INNODB"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_changing_defaults(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 timestamp default 0 "
            ") charset=utf8 engine=INNODB"
        )

        sql2 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 timestamp default CURRENT_TIMESTAMP "
            ") charset=utf8 engine=INNODB"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_changing_other_column(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 timestamp default CURRENT_TIMESTAMP "
            ") charset=utf8 engine=INNODB"
        )

        sql2 = (
            "Create table foo ("
            "column1 bigint NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default 'abc', "
            "column3 int default 999, "
            "column4 timestamp default CURRENT_TIMESTAMP "
            ") charset=utf8 engine=INNODB"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertFalse(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_date_type(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 date default '2000-01-01' "
            ") charset=utf8 engine=INNODB"
        )

        sql2 = (
            "Create table foo ("
            "column1 bigint NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default 'abc', "
            "column3 int default 999, "
            "column4 date default '2000-01-01'"
            ") charset=utf8 engine=INNODB"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertFalse(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_on_update_current(self):
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0, "
            "column4 timestamp default '2000-01-01' "
            ") charset=utf8 engine=INNODB"
        )

        sql2 = (
            "Create table foo ("
            "column1 bigint NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default 'abc', "
            "column3 int default 999, "
            "column4 timestamp default '2000-01-01' on update CURRENT_TIMESTAMP"
            ") charset=utf8 engine=INNODB"
        )

        obj1 = parse_create(sql1)
        obj2 = parse_create(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))
