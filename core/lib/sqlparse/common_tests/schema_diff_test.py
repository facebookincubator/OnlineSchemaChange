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
            sql2 = sql1 + ' {}="{}" '.format(attr, "abc")
            tbl_2 = parse_create(sql2)

            tbl_diff = SchemaDiff(tbl_1, tbl_2)
            self.assertEqual(len(tbl_diff.diffs()["added"]), 1)

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
