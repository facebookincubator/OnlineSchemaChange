#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""


from dba.osc.core.lib.sqlparse.common_tests.schema_diff_test import (
    BaseHelpersTest,
    BaseSQLParserTest,
)
from osc.lib.sqlparse import CreateParser, SchemaDiff


class SQLParserTest(BaseSQLParserTest):
    def setUp(self):
        super().setUp()
        self.parse_function = CreateParser.parse


class HelpersTest(BaseHelpersTest):
    def setUp(self):
        super().setUp()
        self.parse_function = CreateParser.parse

    def test_sql_statement_to_add_partitions_adds_diff_partitions_with_hash(
        self,
    ):
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

    def test_ignore_partition(self):
        """
        Make sure partition difference will be ignored if we pass in
        ignore_partition, vice versa
        """
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = (
            "Create table foo "
            "( column1 varchar(50) CHARACTER SET utf8 COLLATE utf8_bin, "
            " PRIMARY KEY (column1) ) CHARSET=utf8mb3 COLLATE=utf8_bin "
        )
        sql2 = (
            "Create table foo "
            "( column1 varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin, "
            " PRIMARY KEY (column1) ) CHARSET=utf8 COLLATE=utf8mb3_bin "
        )
        tbl_1 = self.parse_function(sql1)
        tbl_2 = self.parse_function(sql2)

        tbl_diff = SchemaDiff(tbl_1, tbl_2, ignore_partition=True)
        self.assertEqual(tbl_diff.to_sql(), None)

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
