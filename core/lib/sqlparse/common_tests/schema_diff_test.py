#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import unittest

from osc.lib.sqlparse import (
    get_type_conv_columns,
    need_default_ts_bootstrap,
    parse_create,
    SchemaDiff,
)


class BaseSQLParserTest(unittest.TestCase):
    def setUp(self):
        self.parse_function = None

    def skipTestIfBaseClass(self, reason):
        if not self.parse_function:
            self.skipTest(reason)

    def test_remove_table_attrs(self):
        self.skipTestIfBaseClass("Need to implement base class")
        tbl_1 = """
        CREATE TABLE `tb1` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4
        """
        tbl_2 = """
        CREATE TABLE `tb1` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """
        tbl_1_obj = self.parse_function(tbl_1)
        tbl_2_obj = self.parse_function(tbl_2)
        tbl_diff = SchemaDiff(tbl_1_obj, tbl_2_obj)
        self.assertEqual(
            "ALTER TABLE `tb1` key_block_size=0, row_format=default",
            tbl_diff.to_sql(),
        )

    def test_two_identical_table(self):
        """
        Two identical table schema shouldn't generate any diff results
        """
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 int )"
        tbl_1 = self.parse_function(sql)
        tbl_2 = self.parse_function(sql)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertFalse(bool(tbl_diff.diffs()["removed"]))
        self.assertFalse(bool(tbl_diff.diffs()["added"]))

    def test_single_col_diff(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = "Create table foo\n" "( column1 int )"
        sql2 = "Create table foo (" " column1 int , " " column2 varchar(10))"
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 0)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

    def test_col_order(self):
        self.skipTestIfBaseClass("Need to implement base class")
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
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["msgs"]), 3)
        self.assertEqual(
            (
                "ALTER TABLE `foo` MODIFY `column1` INT NULL FIRST,"
                " MODIFY `column2` VARCHAR(10) NULL AFTER `column1`"
            ),
            tbl_diff.to_sql(),
        )

        sql_1 = (
            "Create table foo ("
            " column1 varchar(10),"
            " column2 int,"
            " column3 int,"
            " column4 int,"
            " column5 int,"
            " column6 int)"
        )

        sql_2 = (
            "Create table foo ("
            " column1 varchar(10),"
            " column5 int,"
            " column6 int,"
            " column2 int,"
            " column3 int,"
            " column4 int)"
        )
        tbl_1 = self.parse_function(sql_1)
        tbl_2 = self.parse_function(sql_2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(
            (
                "ALTER TABLE `foo` MODIFY `column5` INT NULL AFTER `column1`,"
                " MODIFY `column6` INT NULL AFTER `column5`,"
                " MODIFY `column2` INT NULL AFTER `column6`,"
                " MODIFY `column3` INT NULL AFTER `column2`,"
                " MODIFY `column4` INT NULL AFTER `column3`"
            ),
            tbl_diff.to_sql(),
        )

        sql_2 = (
            "Create table foo ("
            " column1 varchar(10),"
            " column5 int,"
            " column2 int,"
            " column7 int,"
            " column3 int,"
            " column4 int,"
            " column6 int)"
        )

        tbl_2 = self.parse_function(sql_2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(
            (
                "ALTER TABLE `foo` ADD `column7` INT NULL AFTER `column2`,"
                " MODIFY `column5` INT NULL AFTER `column1`,"
                " MODIFY `column2` INT NULL AFTER `column5`,"
                " MODIFY `column3` INT NULL AFTER `column7`,"
                " MODIFY `column4` INT NULL AFTER `column3`"
            ),
            tbl_diff.to_sql(),
        )

    def test_col_order_with_new_col(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = "Create table foo (" " column2 varchar(10) ," " column1 int)"
        sql2 = (
            "Create table foo ("
            " column1 int , "
            " column2 varchar(10) ,"
            " column3 int)"
        )
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["msgs"]), 3)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

    def test_column_type_changed(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = "Create table foo " "( column1 int )"
        sql2 = "Create table foo " "( column1 varchar(10) )"
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will not be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 0)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 0)

    def test_column_default_changed(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = "Create table foo " "( column1 int default 0)"
        sql2 = "Create table foo " "( column1 int default 1)"
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will not be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 0)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 0)

    def test_index_added(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = "Create table foo " "( column1 int default 0)"
        sql2 = "Create table foo( " "column1 int default 0," "index `idx` (column1))"
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        # A modified column will be treated as a combination of remove and add
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

    def test_pri_key_diff(self):
        self.skipTestIfBaseClass("Need to implement base class")
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
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        tbl_3 = self.parse_function(sql3)

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
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = (
            "Create table foo " "( column1 int default 0, " " PRIMARY KEY (column1) )"
        )
        tbl_1 = self.parse_function(sql1)

        for attr in ("charset", "collate"):
            sql2 = sql1 + " {}={} ".format(attr, "abc")
            tbl_2 = self.parse_function(sql2)

            tbl_diff = SchemaDiff(tbl_1, tbl_2)
            self.assertEqual(len(tbl_diff.diffs()["added"]), 1)
        sql2 = sql1 + " key_block_size=123 "
        tbl_2 = self.parse_function(sql2)

        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

        sql2 = sql1 + " charset=abc row_format=compressed "
        tbl_2 = self.parse_function(sql2)

        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 2)

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
        self.skipTestIfBaseClass("Need to implement base class")
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
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)

        tbl_diff = SchemaDiff(tbl_1, tbl_2, ignore_partition=True)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 0)
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 0)

        tbl_diff = SchemaDiff(tbl_1, tbl_2)
        self.assertEqual(len(tbl_diff.diffs()["added"]), 1)
        self.assertEqual(len(tbl_diff.diffs()["removed"]), 1)

    def test_type_conv_columns(self):
        self.skipTestIfBaseClass("Need to implement base class")
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
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)
        type_conv_columns = get_type_conv_columns(tbl_1, tbl_2)

        # Both column type and length change is considered as type conversion
        self.assertEqual(len(type_conv_columns), 2)
        self.assertEqual(type_conv_columns[0].name, "column2")
        self.assertEqual(type_conv_columns[1].name, "column3")

    def test_meta_diff_with_commas(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertNotEqual(obj1, obj2)

        diff_obj = SchemaDiff(obj1, obj2)
        diffs = diff_obj.diffs()
        self.assertEqual(diffs["attrs_modified"], ["comment"])


class BaseHelpersTest(unittest.TestCase):
    def setUp(self):
        self.parse_function = None

    def skipTestIfBaseClass(self, reason):
        if not self.parse_function:
            self.skipTest(reason)

    def test_need_default_ts_bootstrap(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_add_irrelevant_col(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertFalse(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_implicit_ts_default(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_changing_defaults(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_changing_other_column(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertFalse(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_date_type(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertFalse(need_default_ts_bootstrap(obj1, obj2))

    def test_need_default_ts_bootstrap_on_update_current(self):
        self.skipTestIfBaseClass("Need to implement base class")
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

        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertTrue(need_default_ts_bootstrap(obj1, obj2))

    def sql_statement_partitions_helper(
        self,
        old_table_obj,
        new_table_obj,
        resultOptions,
    ):
        success = False
        for option in resultOptions:
            try:
                self.assertEqual(
                    option,
                    SchemaDiff(
                        old_table_obj,
                        new_table_obj,
                        ignore_partition=False,
                    ).to_sql(),
                )
                success = True
            except Exception:
                print("ignore exception for {}", option)

        self.assertEqual(True, success)

    def test_sql_statement_to_add_partitions_adds_diff_partitions_with_hash(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 18"
        )

        options = {"ALTER TABLE `a` ADD PARTITION PARTITIONS 6"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_diff_partitions_with_key(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 18"
        )

        options = {"ALTER TABLE `a` ADD PARTITION PARTITIONS 6"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_drop_partitions_drops_diff_partitions_with_hash(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 10"
        )

        options = {"ALTER TABLE `a` COALESCE PARTITION 2"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_no_sql_statement_when_diff_partitions_is_0_with_hash(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 12"
        )

        options = {None}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_no_sql_statement_when_diff_partitions_is_0_with_key(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 12"
        )

        options = {None}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_drop_partitions_drops_diff_partitions_with_key(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 10"
        )

        options = {"ALTER TABLE `a` COALESCE PARTITION 2"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_both_partitions_with_range(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN (1481400039) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` ADD PARTITION "
            "(PARTITION p1 VALUES LESS THAN (1481313639),"
            " PARTITION p2 VALUES LESS THAN (1481400039))",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_both_partitions_with_range_with_maxvalue(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` ADD PARTITION "
            "(PARTITION p1 VALUES LESS THAN (1481313639),"
            " PARTITION p2 VALUES LESS THAN (MAXVALUE))",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_both_partitions_with_range_with_maxvalue_and_to_days(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "time_updated datetime NOT NULL primary key) "
            " PARTITION BY RANGE (to_days(time_updated)) "
            "(PARTITION p0 VALUES LESS THAN (to_days('2010-11-07')) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "time_updated datetime NOT NULL primary key) "
            " PARTITION BY RANGE (TO_DAYS(time_updated)) "
            "(PARTITION p0 VALUES LESS THAN (to_days('2010-11-07')) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (to_days('2014-11-07')) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` ADD PARTITION "
            "(PARTITION p1 VALUES LESS THAN (to_days('2014-11-07')), "
            "PARTITION p2 VALUES LESS THAN (MAXVALUE))",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_both_partitions_with_list_with_maxvalue_and_to_days(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "time_updated datetime NOT NULL primary key) "
            " PARTITION BY LIST (to_days(time_updated)) "
            "(PARTITION p0 VALUES IN (to_days('2010-11-07')) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "time_updated datetime NOT NULL primary key) "
            " PARTITION BY LIST (TO_DAYS(time_updated)) "
            "(PARTITION p0 VALUES IN (to_days('2010-11-07')) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (to_days('2014-11-07')) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` ADD PARTITION "
            "(PARTITION p1 VALUES IN (to_days('2014-11-07')))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_both_partitions_with_range_with_maxvalue_and_timestamp(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "time_updated datetime NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN ('2010-11-07') ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "time_updated datetime NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN ('2010-11-07') ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN ('2014-11-07') ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` ADD PARTITION "
            "(PARTITION p1 VALUES LESS THAN ('2014-11-07'), "
            "PARTITION p2 VALUES LESS THAN (MAXVALUE))",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_both_partitions_with_comma_list(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630, 1481313631) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630, 1481313631) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639, 1481313640) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES IN (1481400039, 1481400040) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` ADD PARTITION "
            "(PARTITION p1 VALUES IN (1481313639, 1481313640),"
            " PARTITION p2 VALUES IN (1481400039, 1481400040))",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_adds_both_partitions_with_an_element_list(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES IN (1481400039) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` ADD PARTITION "
            "(PARTITION p1 VALUES IN (1481313639),"
            " PARTITION p2 VALUES IN (1481400039))",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_succeeds_with_add_and_drop_partitions_case_1(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p3 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES IN (1481400039) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0 INTO "
            "(PARTITION p3 VALUES IN (1481313630), "
            "PARTITION p1 VALUES IN (1481313639), "
            "PARTITION p2 VALUES IN (1481400039))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_succeeds_with_add_and_drop_partitions_case_2(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION n0 VALUES IN (1481313640) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES IN (1481400039) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION n0 INTO "
            "(PARTITION p1 VALUES IN (1481313639), "
            "PARTITION p2 VALUES IN (1481400039))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_succeeds_with_add_and_drop_partitions_case_3(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p3 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0 INTO "
            "(PARTITION p3 VALUES IN (1481313630), "
            "PARTITION p1 VALUES IN (1481313639))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_add_partitions_succeeds_with_add_and_drop_partitions_case_4(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p3 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p3 VALUES IN (1481313630))",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_with_dropped_partitions_drops_both_partitions_in_range(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN (1481400039) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        options = {
            "ALTER TABLE `a` DROP PARTITION p1, p2",
            "ALTER TABLE `a` DROP PARTITION p2, p1",
        }
        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_with_dropped_partitions_drops_both_partitions_in_list(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES IN (1481400039) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        options = {
            "ALTER TABLE `a` DROP PARTITION p1, p2",
            "ALTER TABLE `a` DROP PARTITION p2, p1",
        }
        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partitions_splits_a_partition_case1(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630, 1481313625) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313625) ENGINE = InnoDB, "
            " PARTITION p2 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0 INTO "
            "(PARTITION p0 VALUES IN (1481313625),"
            " PARTITION p2 VALUES IN (1481313630))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partitions_splits_a_partition_case2(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p1 VALUES LESS THAN (1481313625) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0 INTO "
            "(PARTITION p1 VALUES LESS THAN (1481313625),"
            " PARTITION p2 VALUES LESS THAN (1481313630))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_list_splits_a_partition_case3(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p1 VALUES IN (1481313622) ENGINE = InnoDB, "
            " PARTITION p2 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p1 VALUES IN (1481313622),"
            " PARTITION p2 VALUES IN (1481313630))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_splits_a_partition_case3(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p1 VALUES LESS THAN (1481313620) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p1 VALUES LESS THAN (1481313620),"
            " PARTITION p2 VALUES LESS THAN (1481313630))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_list_merges_a_partition_with_existing_values(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p1 VALUES IN (1481313622, 1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p1 VALUES IN (1481313622, 1481313630))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_merges_a_partition_case1(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p0 VALUES LESS THAN (1481313630))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_merges_partition_case3(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1, p2 INTO "
            "(PARTITION p0 VALUES LESS THAN (1481313630), "
            "PARTITION p3 VALUES LESS THAN (MAXVALUE))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_merges_partition_case2(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1, p2 INTO "
            "(PARTITION p0 VALUES LESS THAN (1481313630), "
            "PARTITION p3 VALUES LESS THAN (1481313631))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_does_not_merge_partition_when_inner_range_is_inequal(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313625) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313628) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )

        options = {None}
        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_does_not_merge_partition_when_last_range_is_smaller(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313625) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313628) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {None}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_does_not_merge_partition_when_inbetween_range_is_smaller(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313625) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313628) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313629) ENGINE = InnoDB, "
            " PARTITION p3 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )

        options = {None}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_list_merges_a_partition_with_additional_values(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p1 VALUES IN (1481313622, 1481313630, 1481313631) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p1 VALUES IN (1481313622, 1481313630, 1481313631))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_merges_a_partition_case4(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313623) ENGINE = InnoDB, "
            " PARTITION p2 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313631) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p1, p2 INTO "
            "(PARTITION p1 VALUES LESS THAN (1481313631))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_list_reshuffles_a_partition(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p1 VALUES IN (1481313622) ENGINE = InnoDB, "
            " PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p1 VALUES IN (1481313622), "
            "PARTITION p0 VALUES IN (1481313630))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_reorganize_partition_range_reshuffles_partitions(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313622) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES LESS THAN (1481313635) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1 INTO "
            "(PARTITION p0 VALUES LESS THAN (1481313630), "
            "PARTITION p1 VALUES LESS THAN (1481313635))"
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_with_dropped_and_renamed_partitions_is_valid_in_list(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES IN (1481400039) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p3 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        options = {
            "ALTER TABLE `a` REORGANIZE PARTITION p0, p1, p2 INTO "
            "(PARTITION p3 VALUES IN (1481313630))",
        }
        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_hash_to_range(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """

        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (\n"
                "PARTITION p0 VALUES LESS THAN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_key_to_range(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """

        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (\n"
                "PARTITION p0 VALUES LESS THAN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_list_to_range(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """

        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY LIST (time_updated) "
            " (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB, "
            " PARTITION p1 VALUES IN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES IN (1481400039) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (\n"
                "PARTITION p0 VALUES LESS THAN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_range_to_list(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY LIST (time_updated) "
            "(PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY LIST (time_updated) (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY LIST (time_updated) (\n"
                "PARTITION p0 VALUES IN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_hash_to_list(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY HASH (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY LIST (time_updated) "
            "(PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY LIST (time_updated) (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY LIST (time_updated) (\n"
                "PARTITION p0 VALUES IN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_key_to_list(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY KEY (time_updated) "
            " PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY LIST (time_updated) "
            "(PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY LIST (time_updated) (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY LIST (time_updated) (\n"
                "PARTITION p0 VALUES IN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_list_to_key(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY LIST (time_updated) "
            "(PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 12"
        )

        options = {
            "ALTER TABLE `a` PARTITION BY KEY (time_updated) PARTITIONS 12",
            # new scheduler
            "ALTER TABLE `a` PARTITION BY KEY (`time_updated`) PARTITIONS 12",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_list_to_hash(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY LIST (time_updated) "
            "(PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 12"
        )

        options = {"ALTER TABLE `a` PARTITION BY HASH (time_updated) PARTITIONS 12"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_hash_to_key(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 12"
        )

        options = {
            "ALTER TABLE `a` PARTITION BY KEY (time_updated) PARTITIONS 12",
            # new scheduler
            "ALTER TABLE `a` PARTITION BY KEY (`time_updated`) PARTITIONS 12",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_key_to_hash(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 12"
        )

        options = {"ALTER TABLE `a` PARTITION BY HASH (time_updated) PARTITIONS 12"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_range_to_key(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 12"
        )

        options = {
            "ALTER TABLE `a` PARTITION BY KEY (time_updated) PARTITIONS 12",
            # new scheduler
            "ALTER TABLE `a` PARTITION BY KEY (`time_updated`) PARTITIONS 12",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_range_to_hash(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 12"
        )

        options = {"ALTER TABLE `a` PARTITION BY HASH (time_updated) PARTITIONS 12"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_none_to_hash(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 12"
        )

        options = {"ALTER TABLE `a` PARTITION BY HASH (time_updated) PARTITIONS 12"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_none_to_key(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 12"
        )

        options = {
            "ALTER TABLE `a` PARTITION BY KEY (time_updated) PARTITIONS 12",
            # new scheduler
            "ALTER TABLE `a` PARTITION BY KEY (`time_updated`) PARTITIONS 12",
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_none_to_range(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY RANGE (time_updated) (\n"
                "PARTITION p0 VALUES LESS THAN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_none_to_list(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY LIST (time_updated) "
            "(PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )

        options = {
            "ALTER TABLE `a` PARTITION BY LIST (time_updated) (PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB)",
            (
                "ALTER TABLE `a` PARTITION BY LIST (time_updated) (\n"
                "PARTITION p0 VALUES IN (1481313630) ENGINE INNODB)"
            ),
        }

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_hash_to_none(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )

        options = {"ALTER TABLE `a` REMOVE PARTITIONING"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_key_to_none(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )

        options = {"ALTER TABLE `a` REMOVE PARTITIONING"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_hash_to_zero_partitions(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY HASH (time_updated) "
            "PARTITIONS 0"
        )

        options = {None}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_key_to_zero_partitions(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 12"
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY KEY (time_updated) "
            "PARTITIONS 0"
        )

        options = {None}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_list_to_none(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY LIST (time_updated) "
            "(PARTITION p0 VALUES IN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )

        options = {"ALTER TABLE `a` REMOVE PARTITIONING"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_sql_statement_to_change_partition_type_from_range_to_none(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
            "PARTITION BY RANGE (time_updated) "
            "(PARTITION p0 VALUES LESS THAN (1481313630) ENGINE = InnoDB) "
        )
        new_table_obj = self.parse_function(
            " CREATE TABLE a "
            "( ID int, "
            "`time_updated` bigint(20) unsigned NOT NULL primary key) "
        )

        options = {"ALTER TABLE `a` REMOVE PARTITIONING"}

        self.sql_statement_partitions_helper(old_table_obj, new_table_obj, options)

    def test_only_change_fks(
        self,
    ):
        self.skipTestIfBaseClass("Need to implement base class")
        old_table_obj = self.parse_function(
            """CREATE TABLE `child` (
                `id` int(11) DEFAULT NULL,
                `parent_id` int(11) DEFAULT NULL,
                KEY `par_ind` (`parent_id`),
                CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`)
                REFERENCES `parent` (`id`) ON DELETE CASCADE,
                CONSTRAINT `child_ibfk_2` FOREIGN KEY (`parent_name`)
                REFERENCES `parent` (`name`),
                CONSTRAINT `child_ibfk_3` FOREIGN KEY (`parent_job`)
                REFERENCES `parent` (`job`)
                ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""
        )
        new_table_obj = self.parse_function(
            """CREATE TABLE `child` (
                `id` int(11) DEFAULT NULL,
                `parent_id` int(11) DEFAULT NULL,
                KEY `par_ind` (`parent_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""
        )

        tbl_diff_1 = SchemaDiff(old_table_obj, new_table_obj)
        tbl_diff_2 = SchemaDiff(new_table_obj, old_table_obj)
        # Only dropping FKs
        self.assertEqual(
            "ALTER TABLE `child` DROP FOREIGN KEY `child_ibfk_1`, "
            "DROP FOREIGN KEY `child_ibfk_2`, "
            "DROP FOREIGN KEY `child_ibfk_3`",
            tbl_diff_1.to_sql(),
        )
        # Only adding FKs
        self.assertEqual(
            None,
            tbl_diff_2.to_sql(),
        )
