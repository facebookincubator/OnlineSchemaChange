#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import time
import unittest
from unittest.mock import MagicMock, Mock

import MySQLdb

from ..lib import constant
from ..lib.error import OSCError
from ..lib.mysql_version import MySQLVersion
from ..lib.payload.cleanup import CleanupPayload
from ..lib.payload.copy import CopyPayload
from ..lib.sqlparse import parse_create


class CopyPayloadTestCase(unittest.TestCase):
    def payload_setup(self, **payload_kwargs):
        payload = CopyPayload(**payload_kwargs)
        table_obj = parse_create(" CREATE TABLE a " "( ID int primary key ) ")
        payload._old_table = table_obj
        payload._new_table = table_obj
        payload._current_db = "test"
        payload.range_start_vars_array = ["@ID"]
        payload.range_end_vars_array = ["@ID"]
        return payload

    def test_init_table_obj_populate_charset_collation(self):
        payload = CopyPayload()
        payload.table_exists = Mock(return_value=True)
        payload.fetch_table_schema = Mock(
            return_value=parse_create(
                """
                CREATE TABLE a (
                ID varchar(32) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=latin1
                """
            )
        )
        payload.fetch_partitions = Mock(return_value=None)
        payload._new_table = parse_create(
            """
            CREATE TABLE a (
            ID varchar(32) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
            """
        )
        payload.get_default_collations = Mock(return_value={"latin1": "latin1_bin"})
        payload.get_collations = Mock(return_value={"latin1_bin": "latin1"})
        payload.init_table_obj()

        explicit_obj = parse_create(
            """
            CREATE TABLE a (
            ID varchar(32) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
            """
        )
        self.assertEqual(payload._old_table, payload._new_table)
        self.assertEqual(payload._old_table, explicit_obj)

        # if the charset is not explicit, we won't populate that
        payload._new_table = parse_create(
            """
            CREATE TABLE a (
            ID varchar(32) NOT NULL
            ) ENGINE=InnoDB COLLATE=latin1_bin
            """
        )
        payload.init_table_obj()
        self.assertNotEqual(payload._old_table, payload._new_table)

    def test_populate_charset_collation(self):
        payload = CopyPayload()
        payload.get_default_collations = Mock(return_value={"latin1": "latin1_bin"})
        payload.get_collations = Mock(return_value={"latin1_bin": "latin1"})
        obj1 = parse_create(
            """
            CREATE TABLE a (
            ID varchar(32) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """
        )
        payload.populate_charset_collation(obj1)
        self.assertEqual(obj1.collate, "latin1_bin")
        self.assertEqual(len(obj1.column_list), 1)
        self.assertEqual(obj1.column_list[0].charset, "latin1")
        self.assertEqual(obj1.column_list[0].collate, "latin1_bin")

        payload.get_default_collations = Mock(
            return_value={"latin1": "latin1_bin", "utf8mb4": "utf8mb4_general_ci"}
        )
        payload.get_collations = Mock(
            return_value={"latin1_bin": "latin1", "utf8mb4_general_ci": "utf8mb4"}
        )
        obj2 = parse_create(
            """
            CREATE TABLE a (
            ID varchar(32) COLLATE utf8mb4_general_ci NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """
        )
        payload.populate_charset_collation(obj2)
        self.assertEqual(obj2.collate, "latin1_bin")
        self.assertEqual(len(obj2.column_list), 1)
        self.assertEqual(obj2.column_list[0].charset, "utf8mb4")
        self.assertEqual(obj2.column_list[0].collate, "utf8mb4_general_ci")

        # would not populate table charset if it's absent
        obj3 = parse_create(
            """
            CREATE TABLE a (
            ID varchar(32) COLLATE utf8mb4_general_ci NOT NULL
            ) ENGINE=InnoDB COLLATE=latin1_bin
            """
        )
        payload.populate_charset_collation(obj3)
        self.assertEqual(obj3.charset, None)

    def test_create_copy_table_populate_charset_collation(self):
        payload = CopyPayload()
        payload._new_table = parse_create(
            """
            CREATE TABLE a (
            ID varchar(32) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
            """
        )
        payload._old_table = payload._new_table
        payload.fail_for_implicit_conv = True
        payload.rm_partition = False
        payload.mysql_version = Mock(is_mysql8=False)
        payload.execute_sql = Mock()
        payload.fetch_partitions = Mock(return_value=None)
        payload.add_drop_table_entry = Mock()
        payload.fetch_table_schema = Mock(
            return_value=parse_create(
                """
            CREATE TABLE a (
            ID varchar(32) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """
            )
        )
        payload.get_default_collations = Mock(return_value={"latin1": "latin1_bin"})
        payload.get_collations = Mock(return_value={"latin1_bin": "latin1"})
        payload.create_copy_table()

    def test_populate_charset_collation_utf8_alias_default_collate(self) -> None:
        payload = CopyPayload()
        payload.get_default_collations = Mock(return_value={"utf8": "utf8_general_ci"})
        payload.get_collations = Mock(return_value={"utf8_general_ci": "utf8"})
        obj1 = parse_create(
            "CREATE TABLE `t1`(s1 CHAR(1)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
        )
        obj2 = parse_create(
            "CREATE TABLE `t1`(s1 CHAR(1)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"
        )
        self.assertEqual(
            payload.populate_charset_collation(obj1),
            payload.populate_charset_collation(obj2),
        )

    def test_populate_charset_collation_utf8_alias_custom_collate(self) -> None:
        payload = CopyPayload()
        payload.get_default_collations = Mock(return_value={"utf8": "utf8_general_ci"})
        payload.get_collations = Mock(
            return_value={"utf8_general_ci": "utf8", "utf8_bin": "utf8"}
        )
        obj1 = parse_create(
            """
            CREATE TABLE `t1`(s1 CHAR(1))
            ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
            """
        )
        obj2 = parse_create(
            """
            CREATE TABLE `t1`(s1 CHAR(1))
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_bin
            """
        )
        self.assertEqual(
            payload.populate_charset_collation(obj1),
            payload.populate_charset_collation(obj2),
        )

    def test_checksum_running_with_proper_idx(self):
        payload = CopyPayload()
        payload._new_table = Mock(indexes=[])
        payload._old_table = Mock(indexes=[])
        pri_key_list = []
        for i in range(3):
            col = Mock()
            col.name = "col{}".format(i)
            pri_key_list.append(col)

        payload._old_table.primary_key = Mock(is_unique=True)
        payload._old_table.primary_key.name = "PRIMARY"
        payload._old_table.primary_key.column_list = pri_key_list

        payload._new_table.primary_key = Mock(is_unique=True)
        payload._new_table.primary_key.name = "PRIMARY"
        payload._new_table.primary_key.column_list = pri_key_list

        # If primary key hasn't been changed, we can use that one for checksum
        payload._pk_for_filter = [c.name for c in pri_key_list]
        self.assertEqual(payload.find_coverage_index(), "PRIMARY")

        # If new primary key has its left most prefix covering the old primary
        # key, we can use that as well
        col = Mock()
        col.name = "col4"
        pri_key_list.append(col)
        payload._new_table.primary_key.column_list = pri_key_list
        self.assertEqual(payload.find_coverage_index(), "PRIMARY")

        # If new primary key has its left most prefix covering the old primary
        # key, but the sequence is different, then we cannot use that
        pri_key_list = []
        for i in range(2, -1, -1):
            col = Mock()
            col.name = "col{}".format(i)
            pri_key_list.append(col)
        payload._new_table.primary_key.column_list = pri_key_list
        self.assertEqual(payload.find_coverage_index(), None)

    def test_replay_gap_will_be_filled(self):
        """
        Make sure whenever there's a visiable gap in _chg table, we will
        return it through get_gap_changes, and filling the point.
        """
        payload = self.payload_setup()
        payload._replayed_chg_ids.extend([1, 2, 4, 5])
        delta = [{payload.IDCOLNAME: 3}]
        payload.query = Mock(return_value=delta)
        # We will get whatever gap returned from MySQL database
        self.assertEqual(payload.get_gap_changes(), delta)
        # No more missing points after replay
        self.assertEqual(payload._replayed_chg_ids.missing_points(), [])

    def test_set_innodb_tmpdir(self):
        """
        Make sure set_innodb_tmpdir will catch and only catch 1231 error
        """

        payload = CopyPayload()
        table_obj = parse_create(" CREATE TABLE a " "( ID int primary key ) ")
        payload._old_table = table_obj
        payload._new_table = table_obj

        payload.replay_changes = Mock()
        payload.execute_sql = Mock(side_effect=MySQLdb.OperationalError(1231, "abc"))

        # Call the function make sure it catch the 1231 error
        payload.set_innodb_tmpdir("mock/path")

        # Call the function make sure it will still raise anything other than
        # 1231
        with self.assertRaises(MySQLdb.OperationalError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1111, "abc")
            )
            payload.set_innodb_tmpdir("mock/path")
        self.assertEqual(err_context.exception.args[0], 1111)

    def test_long_selects_being_killed(self):
        # limit wait time to 10 ms
        payload = self.payload_setup(lock_max_wait_before_kill_seconds=0.01)
        mocked_conn = Mock()
        payload.get_conn = Mock(return_value=mocked_conn)
        # execute_sql takes 500 ms to return, more than the 10 ms limit
        payload.execute_sql = Mock(side_effect=lambda _: time.sleep(0.5))

        query_id = 100
        mocked_conn.get_running_queries = Mock(
            return_value=[
                {"Info": b"SELECT 1 from a", "db": "test", "Id": query_id},
                {"Info": b"SELECT 1 from `a`", "db": "test", "Id": query_id + 1},
                {
                    "Info": b"alter table a add column `bar` text",
                    "db": "test",
                    "Id": query_id + 2,
                },
                {"Info": b"select 1 from b", "db": "test", "Id": query_id + 3},
                {"Info": b"select 1 from `b`", "db": "test", "Id": query_id + 4},
                {"Info": b"SELECT 1 from c", "db": "test", "Id": query_id + 5},
                {"Info": b"SELECT 1 from `c`", "db": "test", "Id": query_id + 6},
                {
                    "Info": b"SELECT 1 from a",
                    "db": "information_schema",
                    "Id": query_id + 7,
                },
                {
                    "Info": b"SELECT 1 from `a`",
                    "db": "information_schema",
                    "Id": query_id + 8,
                },
            ]
        )

        # Try lock tables
        payload.lock_tables(tables=["a", "b"])

        # Be sure that the kill timer is finished
        payload._last_kill_timer.join(1)
        self.assertFalse(payload._last_kill_timer.is_alive())

        # Make sure kill selects only on tables a and b
        kill_calls = mocked_conn.kill_query_by_id.call_args_list
        self.assertEquals(len(kill_calls), 5)
        for idx, killed in enumerate(
            (query_id, query_id + 1, query_id + 2, query_id + 3, query_id + 4)
        ):
            args, kwargs = kill_calls[idx]
            self.assertEquals(len(args), 1)
            self.assertEquals(args[0], killed)

    def test_selects_not_being_killed(self):
        # limit wait time to 1 sec
        payload = self.payload_setup(lock_max_wait_before_kill_seconds=1)
        mocked_conn = Mock()
        payload.get_conn = Mock(return_value=mocked_conn)
        # execute_sql now returns right away
        payload.execute_sql = Mock()

        query_id = 100
        mocked_conn.get_running_queries = Mock(
            return_value=[
                {"Info": b"SELECT 1 from a", "db": "test", "Id": query_id},
                {"Info": b"SELECT 1 from `a`", "db": "test", "Id": query_id + 1},
                {
                    "Info": b"alter table a add column `bar` text",
                    "db": "test",
                    "Id": query_id + 2,
                },
                {"Info": b"select 1 from b", "db": "test", "Id": query_id + 3},
                {"Info": b"select 1 from `b`", "db": "test", "Id": query_id + 4},
                {"Info": b"SELECT 1 from c", "db": "test", "Id": query_id + 5},
                {"Info": b"SELECT 1 from `c`", "db": "test", "Id": query_id + 6},
                {
                    "Info": b"SELECT 1 from a",
                    "db": "information_schema",
                    "Id": query_id + 7,
                },
                {
                    "Info": b"SELECT 1 from `a`",
                    "db": "information_schema",
                    "Id": query_id + 8,
                },
            ]
        )

        # Try lock tables
        payload.lock_tables(tables=["a", "b"])

        # Be sure that the kill timer is finished
        payload._last_kill_timer.join(1)
        self.assertFalse(payload._last_kill_timer.is_alive())

        # Make sure no selects were killed
        mocked_conn.kill_query_by_id.assert_not_called()

    def test_set_rocksdb_bulk_load(self):
        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a " "( ID int primary key ) ENGINE=ROCKSDB"
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        payload.execute_sql = Mock()
        payload.change_rocksdb_bulk_load()
        self.assertTrue(payload.execute_sql.called)

        table_obj = parse_create(
            " CREATE TABLE a " "( ID int primary key ) ENGINE=ROCKSDB"
        )
        new_table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int, id2 int, "
            "primary key (ID,id2)) ENGINE=ROCKSDB"
        )
        payload._old_table = table_obj
        payload._new_table = new_table_obj
        payload.execute_sql = Mock()
        payload.change_rocksdb_bulk_load()
        self.assertFalse(payload.execute_sql.called)

        table_obj = parse_create(
            " CREATE TABLE a " "( ID int primary key ) ENGINE=ROCKSDB"
        )
        payload._old_table = table_obj
        payload._new_table = table_obj

        payload.execute_sql = Mock(side_effect=MySQLdb.OperationalError(1193, "abc"))
        payload.change_rocksdb_bulk_load()

    def test_skip_cleanup(self):
        payload = CopyPayload()
        sql = "CREATE TABLE abc (ID int)"
        database = "db"
        payload._old_table = Mock()
        payload._old_table.name = "abc"
        payload._new_table = Mock()
        payload._new_table.name = "abc"
        payload.outfile_dir = "/path/to/file/dump"
        payload.outfile_suffix_end = 2

        # add some drop table entry pretending we've done some work
        payload._cleanup_payload = CleanupPayload(db=database)
        payload._cleanup_payload.add_drop_table_entry(
            database, constant.DELTA_TABLE_PREFIX + "abc"
        )
        payload._cleanup_payload.add_drop_table_entry(
            database, constant.NEW_TABLE_PREFIX + "abc"
        )
        payload._cleanup_payload.cleanup = Mock()
        for suffix in range(1, payload.outfile_suffix_end + 1):
            payload._cleanup_payload.add_file_entry(payload.outfile + "." + str(suffix))

        # If we don't skip cleanup, then we should have 2 tables to clean up
        payload.skip_cleanup_after_kill = False
        with self.assertRaises(OSCError) as err_context:
            payload.init_connection = Mock(
                side_effect=MySQLdb.OperationalError(2006, "MySQL has gone away")
            )
            payload.run_ddl(database, sql)
        self.assertEqual(len(payload._cleanup_payload.to_drop), 2)
        self.assertEqual(
            len(payload._cleanup_payload.files_to_clean), payload.outfile_suffix_end
        )
        self.assertEqual(err_context.exception.err_key, "GENERIC_MYSQL_ERROR")

        # If we are skipping cleanup, then there's nothing to cleanup
        payload.skip_cleanup_after_kill = True
        with self.assertRaises(OSCError) as err_context:
            payload.init_connection = Mock(
                side_effect=MySQLdb.OperationalError(2006, "MySQL has gone away")
            )
            payload.run_ddl(database, sql)
        # There should be no cleanup entry at all if we skip the table cleanup
        self.assertEqual(payload._cleanup_payload.to_drop, [])
        self.assertEqual(len(payload._cleanup_payload.files_to_clean), 0)
        self.assertEqual(err_context.exception.err_key, "GENERIC_MYSQL_ERROR")

    def test_file_exists(self):
        payload = self.payload_setup()
        with self.assertRaises(OSCError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1086, "abc")
            )
            payload.select_full_table_into_outfile()
        self.assertEqual(err_context.exception.err_key, "FILE_ALREADY_EXIST")

        with self.assertRaises(OSCError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1086, "abc")
            )
            payload.select_chunk_into_outfile("path/to/outfile", False)
        self.assertEqual(err_context.exception.err_key, "FILE_ALREADY_EXIST")

        # Any mysql error other than 1086 should surface
        with self.assertRaises(MySQLdb.OperationalError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1111, "abc")
            )
            payload.select_chunk_into_outfile("path/to/outfile", False)
        self.assertEqual(err_context.exception.args[0], 1111)

    def test_partitions_being_added(self):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """

        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " /*!50100 PARTITION BY RANGE (time_updated) "
            " (PARTITION p1 VALUES LESS THAN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN (1481400039) ENGINE = InnoDB, "
            "  PARTITION p3 VALUES LESS THAN (1481486439) ENGINE = InnoDB)*/"
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        partitions = ["p1", "p2", "p3"]

        # No difference between old and new
        payload.query = Mock(return_value=None)
        # No-op for create table
        payload.execute_sql = Mock()
        payload.fetch_partitions = Mock(return_value=partitions)
        # We will get whatever gap returned from MySQL database
        payload._cleanup_payload.add_drop_table_entry = Mock()
        payload.create_copy_table()
        payload._cleanup_payload.add_drop_table_entry.assert_called_with(
            payload._current_db, payload.new_table_name, partitions
        )

    def test_sql_statement_generated_due_to_added_partitions_adds_both_partitions(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """

        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p1 VALUES LESS THAN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN (1481400039) ENGINE = InnoDB) "
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        partitions = ["p1", "p2"]

        # No difference between old and new
        payload.query = Mock(return_value=None)
        # No-op for create table
        payload.execute_sql = Mock()

        def partition_list_names_mock(*args, **kwargs):
            if args[0] == "a":
                return partitions
            elif args[0] == payload.new_table_name:
                return {}

        def partition_value_for_name_mock(*args, **kwargs):
            if args[1] == "p1":
                return "1481313639"
            if args[1] == "p2":
                return "1481400039"

        payload.get_partition_method = Mock(return_value="RANGE")
        payload.list_partition_names = MagicMock(side_effect=partition_list_names_mock)
        payload.partition_value_for_name = MagicMock(
            side_effect=partition_value_for_name_mock
        )
        payload.rm_partition = "Override"
        payload.partitions = partitions
        payload.sync_table_partitions()
        options = {
            "ALTER TABLE `__osc_new_a` ADD PARTITION "
            "(PARTITION p2 VALUES LESS THAN (1481400039),"
            " PARTITION p1 VALUES LESS THAN (1481313639))",
            "ALTER TABLE `__osc_new_a` ADD PARTITION "
            "(PARTITION p1 VALUES LESS THAN (1481313639),"
            " PARTITION p2 VALUES LESS THAN (1481400039))",
        }

        success = False
        for option in options:
            try:
                payload.execute_sql.assert_called_with(option)
                success = True
            except Exception:
                print("ignore exception {}", option)

        self.assertEqual(True, success)

    def test_sql_statement_generated_due_to_dropped_partitions_drops_both_partitions(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """

        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p1 VALUES LESS THAN (1481313639) ENGINE = InnoDB, "
            "  PARTITION p2 VALUES LESS THAN (1481400039) ENGINE = InnoDB) "
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        partitions = ["p1", "p2"]

        # No difference between old and new
        payload.query = Mock(return_value=None)
        # No-op for create table
        payload.execute_sql = Mock()

        def partition_list_names_mock(*args, **kwargs):
            if args[0] == "a":
                return {}
            elif args[0] == payload.new_table_name:
                return partitions

        def partition_value_for_name_mock(*args, **kwargs):
            if args[1] == "p1":
                return "1481313639"
            if args[1] == "p2":
                return "1481400039"

        payload.get_partition_method = Mock(return_value="RANGE")
        payload.list_partition_names = MagicMock(side_effect=partition_list_names_mock)
        payload.partition_value_for_name = MagicMock(
            side_effect=partition_value_for_name_mock
        )
        payload.rm_partition = "Override"
        payload.partitions = partitions
        payload.sync_table_partitions()
        options = {
            "ALTER TABLE `__osc_new_a` DROP PARTITION p1, p2",
            "ALTER TABLE `__osc_new_a` DROP PARTITION p2, p1",
        }

        success = False
        for option in options:
            try:
                payload.execute_sql.assert_called_with(option)
                success = True
            except Exception:
                print("ignore exception {}", option)

        self.assertEqual(True, success)

    def test_sql_statement_generated_with_added_removed_partitions(
        self,
    ):
        """
        Make sure a partitioned shadow table will always be dropped by
        partitions instead of the whole table
        """

        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key, "
            "`time_updated` bigint(20) unsigned NOT NULL) "
            " PARTITION BY RANGE (time_updated) "
            " (PARTITION p1 VALUES LESS THAN (1481313639) ENGINE = InnoDB) "
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        partitions = ["p1"]
        oldPartitions = ["p2"]

        # No difference between old and new
        payload.query = Mock(return_value=None)
        # No-op for create table
        payload.execute_sql = Mock()

        def partition_list_names_mock(*args, **kwargs):
            if args[0] == "a":
                return partitions
            elif args[0] == payload.new_table_name:
                return oldPartitions

        def partition_value_for_name_mock(*args, **kwargs):
            if args[1] == "p1":
                return "1481313639"
            if args[1] == "p2":
                return "1481400039"

        payload.get_partition_method = Mock(return_value="RANGE")
        payload.list_partition_names = MagicMock(side_effect=partition_list_names_mock)
        payload.partition_value_for_name = MagicMock(
            side_effect=partition_value_for_name_mock
        )
        payload.rm_partition = "Override"
        payload.partitions = partitions
        payload.sync_table_partitions()
        payload.execute_sql.assert_any_call(
            "ALTER TABLE `__osc_new_a` ADD PARTITION "
            "(PARTITION p1 VALUES LESS THAN (1481313639))"
        )
        payload.execute_sql.assert_called_with(
            "ALTER TABLE `__osc_new_a` DROP PARTITION p2"
        )

    def test_dropped_columns(self):
        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( id1 int ,  "
            "id2 int,"
            "col1 varchar(10), "
            "col2 varchar(10), "
            "PRIMARY KEY (id1, id2))"
        )
        table_obj_pri_dropped = parse_create(
            " CREATE TABLE a "
            "( id1 int ,  "
            "col1 varchar(10), "
            "col2 varchar(10), "
            "PRIMARY KEY (id1))"
        )
        table_obj_col_dropped = parse_create(
            " CREATE TABLE a "
            "( id1 int ,  "
            "id2 int,"
            "col1 varchar(10), "
            "PRIMARY KEY (id1, id2))"
        )
        table_obj_both_dropped = parse_create(
            " CREATE TABLE a " "( id1 int ,  " "col1 varchar(10), " "PRIMARY KEY (id1))"
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        # No change in the schema
        self.assertEqual(payload.dropped_column_name_list, [])

        payload._new_table = table_obj_pri_dropped
        self.assertEqual(payload.dropped_column_name_list, ["id2"])

        payload._new_table = table_obj_col_dropped
        self.assertEqual(payload.dropped_column_name_list, ["col2"])

        payload._new_table = table_obj_both_dropped
        self.assertEqual(payload.dropped_column_name_list, ["id2", "col2"])

    def test_checksum_column_list(self):
        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key,  "
            "col1 varchar(10), "
            "col2 varchar(10)) "
        )
        table_obj_new = parse_create(
            " CREATE TABLE a "
            "( ID int primary key,  "
            "col1 varchar(10), "
            "col2 varchar(100)) "
        )
        table_obj_dropped = parse_create(
            " CREATE TABLE a " "( ID int primary key,  " "col2 varchar(100)) "
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        # No change in the schema
        self.assertEqual(payload.checksum_column_list, ["col1", "col2"])

        # changed column being kept
        payload._new_table = table_obj_new
        payload.skip_checksum_for_modified = False
        self.assertEqual(payload.checksum_column_list, ["col1", "col2"])

        # skip changed
        payload._new_table = table_obj_new
        payload.skip_checksum_for_modified = True
        self.assertEqual(payload.checksum_column_list, ["col1"])

        # skip dropped
        payload._new_table = table_obj_dropped
        payload.skip_checksum_for_modified = False
        self.assertEqual(payload.checksum_column_list, ["col2"])

        # skip dropped
        payload._new_table = table_obj_dropped
        payload.skip_checksum_for_modified = False
        self.assertEqual(payload.checksum_column_list, ["col2"])

    def test_parse_session_overrides_str_empty(self):
        payload = self.payload_setup()
        overrides_str = ""
        expected_overrides = []
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_num(self):
        payload = self.payload_setup()
        overrides_str = "var1=1"
        expected_overrides = [["var1", "1"]]
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_str(self):
        payload = self.payload_setup()
        overrides_str = "var1=v"
        expected_overrides = [["var1", "v"]]
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_list(self):
        payload = self.payload_setup()
        overrides_str = "var1=v;var2=1"
        expected_overrides = [["var1", "v"], ["var2", "1"]]
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_malform(self):
        payload = self.payload_setup()
        overrides_str = "var1=v;var2="
        with self.assertRaises(OSCError) as err_context:
            payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(err_context.exception.err_key, "INCORRECT_SESSION_OVERRIDE")

    def test_execute_sql_not_called_for_empty_overrides(self):
        # we shouldn't execute any sql if there's no session overrides
        payload = self.payload_setup()
        payload.execute_sql = Mock()
        payload.session_overrides_str = ""
        payload.override_session_vars()
        self.assertFalse(payload.execute_sql.called)

    def test_not_skip_affected_rows_check(self):
        # Exception should be raised if we do not skip affected_rows check
        # and 0 is returned
        payload = self.payload_setup()
        payload.execute_sql = Mock(return_value=0)
        row = {payload.IDCOLNAME: 1}
        with self.assertRaises(OSCError) as err_context:
            payload.replay_insert_row(row)
        self.assertEqual(err_context.exception.err_key, "REPLAY_WRONG_AFFECTED")

    def test_skip_affected_rows_check(self):
        # No exception should be raised if we skip affected_rows check and 0
        # is returned
        payload = self.payload_setup()
        payload.skip_affected_rows_check = True
        payload.execute_sql = Mock(return_value=0)
        row = {payload.IDCOLNAME: 1}
        payload.replay_insert_row(row)

    def test_is_rbr_safe_stmt(self):
        # is_trigger_rbr_safe should always be True if STATEMENT binlog_format
        # is being used
        payload = self.payload_setup()
        payload.mysql_vars["binlog_format"] = "STATEMENT"
        payload.mysql_version = MySQLVersion("5.1.1")
        self.assertTrue(payload.is_trigger_rbr_safe)

    def test_is_rbr_safe_row_fb(self):
        # is_trigger_rbr_safe should always be True if Facebook version
        # is being used and sql_log_bin_triggers is OFF
        payload = self.payload_setup()
        payload.mysql_vars["binlog_format"] = "ROW"
        payload.mysql_vars["sql_log_bin_triggers"] = "OFF"
        payload.mysql_version = MySQLVersion("5.1.1-fb")
        self.assertTrue(payload.is_trigger_rbr_safe)

    def test_is_rbr_safe_row_fb_but_logs_on(self):
        # is_trigger_rbr_safe should False if we are using a Facebook version
        # but sql_log_bin_triggers is still enabled
        payload = self.payload_setup()
        payload.mysql_vars["binlog_format"] = "ROW"
        payload.mysql_vars["sql_log_bin_triggers"] = "ON"
        payload.mysql_version = MySQLVersion("5.1.1-fb")
        self.assertFalse(payload.is_trigger_rbr_safe)

    def test_is_rbr_safe_row_other_forks(self):
        # is_trigger_rbr_safe should False if we are using a Facebook version
        # but sql_log_bin_triggers is still enabled
        payload = self.payload_setup()
        payload.mysql_vars["binlog_format"] = "ROW"
        payload.mysql_version = MySQLVersion("5.5.30-percona")
        self.assertFalse(payload.is_trigger_rbr_safe)

    def test_divide_changes_all_the_same_type(self):
        payload = CopyPayload()
        payload.replay_group_size = 100
        type_name = payload.DMLCOLNAME
        id_name = payload.IDCOLNAME
        chg_rows = [
            {type_name: 1, id_name: 1},
            {type_name: 1, id_name: 2},
            {type_name: 1, id_name: 3},
            {type_name: 1, id_name: 4},
            {type_name: 1, id_name: 5},
        ]
        groups = list(payload.divide_changes_to_group(chg_rows))
        self.assertEqual(len(groups), 1)
        chg_type, group = groups[0]
        self.assertEqual(chg_type, 1)
        self.assertEqual(group, [1, 2, 3, 4, 5])

    def test_divide_changes_no_change(self):
        payload = CopyPayload()
        payload.replay_group_size = 100
        chg_rows = []
        groups = list(payload.divide_changes_to_group(chg_rows))
        self.assertEqual(len(groups), 0)

    def test_divide_changes_all_different(self):
        """
        If all the changes are different from the previous one, they should
        be put into different groups
        """
        payload = CopyPayload()
        payload.replay_group_size = 100
        type_name = payload.DMLCOLNAME
        id_name = payload.IDCOLNAME
        chg_rows = [
            {type_name: 1, id_name: 1},
            {type_name: 2, id_name: 2},
            {type_name: 3, id_name: 3},
            {type_name: 1, id_name: 4},
            {type_name: 2, id_name: 5},
            {type_name: 3, id_name: 6},
        ]
        groups = list(payload.divide_changes_to_group(chg_rows))
        self.assertEqual(
            groups,
            [
                (1, [1]),
                (2, [2]),
                (3, [3]),
                (1, [4]),
                (2, [5]),
                (3, [6]),
            ],
        )

    def test_divide_changes_simple_group(self):
        payload = CopyPayload()
        payload.replay_group_size = 100
        type_name = payload.DMLCOLNAME
        id_name = payload.IDCOLNAME
        chg_rows = [
            {type_name: 1, id_name: 1},
            {type_name: 2, id_name: 2},
            {type_name: 2, id_name: 3},
            {type_name: 2, id_name: 4},
            {type_name: 1, id_name: 5},
        ]
        groups = list(payload.divide_changes_to_group(chg_rows))
        self.assertEqual(
            groups,
            [
                (1, [1]),
                (2, [2, 3, 4]),
                (1, [5]),
            ],
        )

    def test_divide_changes_no_grouping_for_update(self):
        """
        UPDATE dml type should not been grouped
        """
        payload = CopyPayload()
        payload.replay_group_size = 100
        type_name = payload.DMLCOLNAME
        id_name = payload.IDCOLNAME
        chg_rows = [
            {type_name: 1, id_name: 1},
            {type_name: 3, id_name: 2},
            {type_name: 3, id_name: 3},
            {type_name: 3, id_name: 4},
            {type_name: 1, id_name: 5},
        ]
        groups = list(payload.divide_changes_to_group(chg_rows))
        self.assertEqual(
            groups,
            [
                (1, [1]),
                (3, [2]),
                (3, [3]),
                (3, [4]),
                (1, [5]),
            ],
        )

    def test_divide_changes_group_size_reach_limit(self):
        """
        If group size has exceeded the limit, we should break them into two
        groups
        """
        payload = CopyPayload()
        payload.replay_group_size = 2
        type_name = payload.DMLCOLNAME
        id_name = payload.IDCOLNAME
        chg_rows = [
            {type_name: 1, id_name: 1},
            {type_name: 2, id_name: 2},
            {type_name: 2, id_name: 3},
            {type_name: 2, id_name: 4},
            {type_name: 1, id_name: 5},
        ]
        groups = list(payload.divide_changes_to_group(chg_rows))
        self.assertEqual(
            groups,
            [
                (1, [1]),
                (2, [2, 3]),
                (2, [4]),
                (1, [5]),
            ],
        )

    def test_is_myrocks_table(self):
        payload = CopyPayload()
        payload._new_table = parse_create(
            "CREATE TABLE abc ( " "id int primary key " ") ENGINE = RocksDB "
        )
        self.assertTrue(payload.is_myrocks_table)

    def test_is_myrocks_table_for_innodb(self):
        payload = CopyPayload()
        payload._new_table = parse_create(
            "CREATE TABLE abc ( " "id int primary key " ") ENGINE = InnoDB "
        )
        self.assertFalse(payload.is_myrocks_table)

    def test_myrocks_table_skip_foreign_key_check(self):
        payload = CopyPayload()
        payload._new_table = parse_create(
            "CREATE TABLE abc ( " "id int primary key " ") ENGINE = RocksDB "
        )
        payload.query = Mock()
        payload.foreign_key_check()
        self.assertFalse(payload.query.called)

    def test_wait_for_slow_query_none(self):
        # If there's no slow query, we are expecting True being returned from
        # the function
        payload = self.payload_setup()
        payload.get_long_trx = Mock(return_value=None)
        result = payload.wait_until_slow_query_finish()
        self.assertTrue(result)

    def test_wait_for_slow_query_never_finish(self):
        # If the slow query never finishes, then an OSCError should be raised
        payload = self.payload_setup()
        payload.max_wait_for_slow_query = 1
        payload.get_long_trx = Mock(
            return_value={
                "Time": 100,
                "db": "mydb",
                "Id": 123,
                "Info": "select * from a",
            }
        )
        with self.assertRaises(OSCError) as err_context:
            payload.wait_until_slow_query_finish()
        self.assertEqual(err_context.exception.err_key, "LONG_RUNNING_TRX")

    def test_high_pri_ddl_does_not_wait_for_slow_query(self):
        payload = self.payload_setup()
        payload.stop_slave_sql = Mock()
        payload.ddl_guard = Mock()
        payload.mysql_version = MySQLVersion("8.0.1-fb-1")
        payload.get_conn = Mock()
        payload.execute_sql = Mock()
        payload.wait_until_slow_query_finish = Mock()
        payload.create_triggers()
        self.assertTrue(payload.is_high_pri_ddl_supported)
        payload.wait_until_slow_query_finish.assert_not_called()

        # If high pri ddl is not supported, we should call wait_until_slow_query_finish
        payload.get_long_trx = Mock(return_value=False)
        payload.mysql_version = MySQLVersion("8.0.1-test-1")
        payload.wait_until_slow_query_finish = Mock(return_value=True)
        self.assertFalse(payload.is_high_pri_ddl_supported)
        payload.create_triggers()
        payload.wait_until_slow_query_finish.assert_called_once()

    def test_auto_table_collation_population(self):
        payload = self.payload_setup()
        sql = """
        CREATE TABLE abc (
        ID int primary key
        ) charset = latin1
        """
        payload._new_table = parse_create(sql)
        default_collate = "latin1_swedish_ci"
        payload.get_default_collations = Mock(return_value={"latin1": default_collate})
        payload.get_collations = Mock(return_value={default_collate: "latin1"})
        payload.populate_charset_collation(payload._new_table)
        self.assertEqual(payload._new_table.collate, "latin1_swedish_ci")

    def test_auto_table_charset_population(self):
        payload = self.payload_setup()
        sql = """
        CREATE TABLE abc (
        ID int primary key
        ) collate = latin1_swedish_ci
        """
        payload._new_table = parse_create(sql)
        default_collate = "latin1_swedish_ci"
        payload.get_default_collations = Mock(return_value={"latin1": default_collate})
        payload.get_collations = Mock(return_value={default_collate: "latin1"})
        payload.populate_charset_collation(payload._new_table)

        # charset should not be populated if only collate is provided
        self.assertEqual(payload._new_table.charset, None)

    def test_auto_removal_of_using_hash(self):
        payload = self.payload_setup()
        sql1 = """
        CREATE TABLE abc (
        ID int primary key,
        A varchar(10) not null default '',
        B varchar(20) not null default '',
        KEY `ab` (`A`, `B`) USING HASH
        )
        """
        sql2 = """
        CREATE TABLE abc (
        ID int primary key,
        A varchar(10) not null default '',
        B varchar(20) not null default '',
        KEY `ab` (`A`, `B`)
        )
        """
        payload._new_table = parse_create(sql1)
        payload.remove_using_hash_for_80()
        self.assertEqual(payload._new_table, parse_create(sql2))

    """
    Following test disabled until the high_pri_ddl is fixed
    def test_is_high_pri_ddl_supported_yes_8_0(self):
        payload = self.payload_setup()
        payload.mysql_version = MySQLVersion('8.0.1-fb')
        self.assertTrue(payload.is_high_pri_ddl_supported)

    def test_is_high_pri_ddl_supported_yes_5_6_88(self):
        payload = self.payload_setup()
        payload.mysql_version = MySQLVersion('5.6.88-fb')
        self.assertTrue(payload.is_high_pri_ddl_supported)

    def test_is_high_pri_ddl_supported_yes_5_7(self):
        payload = self.payload_setup()
        payload.mysql_version = MySQLVersion('5.7.1-fb')
        self.assertTrue(payload.is_high_pri_ddl_supported)

    def test_is_high_pri_ddl_supported_no(self):
        payload = self.payload_setup()
        payload.mysql_version = MySQLVersion('5.6.1-fb')
        self.assertFalse(payload.is_high_pri_ddl_supported)

    def test_is_high_pri_ddl_supported_no_for_non_fb(self):
        payload = self.payload_setup()
        payload.mysql_version = MySQLVersion('5.7.1')
        self.assertFalse(payload.is_high_pri_ddl_supported)

    """

    def test_detailed_checksum(self):
        payload = self.payload_setup()
        payload.find_coverage_index = Mock()
        payload.dump_current_chunk = Mock()
        payload.checksum_for_single_chunk = Mock(
            return_value={"col1": "abce123", "col2": "fghi456", "_osc_chunk_cnt": 0}
        )

        # No error should be raised if there's no mismatch
        payload.detailed_checksum()
        self.assertFalse(payload.dump_current_chunk.called)

    def test_detailed_checksum_mismatch(self):
        payload = self.payload_setup()
        payload.find_coverage_index = Mock()
        payload.dump_current_chunk = Mock()
        payload.checksum_for_single_chunk = Mock(
            side_effect=[
                {"col1": "abcd123", "col2": "fghi456", "_osc_chunk_cnt": 0},
                {"col1": "123abcd", "col2": "fghi456", "_osc_chunk_cnt": 0},
            ]
        )

        # Error should be raised if there's an mismatch
        with self.assertRaises(OSCError):
            payload.detailed_checksum()
            self.assertTrue(payload.dump_current_chunk.called)


class CopyPayloadPKFilterTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = CopyPayload()
        self.table_obj_1pk = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "id2 int, "
            "id3 int, "
            "PRIMARY KEY(id1) "
            ") "
        )
        self.table_obj_2pk = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "id2 int, "
            "id3 int, "
            "PRIMARY KEY(id1, id2) "
            ") "
        )
        self.table_obj_uk = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "id2 int, "
            "id3 int, "
            "UNIQUE KEY(id1) "
            ") "
        )
        self.table_obj_no_uk = parse_create(
            " CREATE TABLE a (" "id1 int, " "id2 int, " "id3 int " ") "
        )
        self.table_obj_newcol = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "id2 int, "
            "id3 int, "
            "dummy int,"
            "PRIMARY KEY(id1) "
            ") "
        )
        self.table_obj_newcol_with_idx = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "id2 int, "
            "id3 int, "
            "dummy int, "
            "PRIMARY KEY(id1), "
            "KEY (dummy) "
            ") "
        )

        self.table_obj_pk1_prefixed = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "name varchar(200) NOT NULL,"
            "dummy1 int, "
            "PRIMARY KEY (id, name(42)) "
            ") "
        )
        self.table_obj_pk2_prefixed = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "name varchar(200) NOT NULL,"
            "dummy1 int, "
            "dummy2 int, "
            "PRIMARY KEY (id, name(42)) "
            ") "
        )

        self.table_obj_pk3_subset = parse_create(
            " CREATE TABLE a ("
            "id1 int, "
            "name varchar(200) NOT NULL,"
            "dummy1 int, "
            "dummy2 int, "
            "PRIMARY KEY (id1) "
            ") "
        )

        self.payload._current_db = "test"

    def test_decide_pk_for_filter_1pk_to_2pk(self):
        # Adding new columns to pk should still use old pk for filtering
        self.payload._old_table = self.table_obj_1pk
        self.payload._new_table = self.table_obj_2pk
        self.payload.decide_pk_for_filter()
        self.assertEquals(self.payload._pk_for_filter, ["id1"])

    def test_decide_pk_for_filter_uk_to_2pk(self):
        # An UK should be used if there's no existing pk
        self.payload._old_table = self.table_obj_uk
        self.payload._new_table = self.table_obj_2pk
        self.payload.decide_pk_for_filter()
        self.assertEquals(self.payload._pk_for_filter, ["id1"])

    def test_decide_pk_for_filter_no_uk_allow(self):
        # An UK should be used if there's no existing pk
        self.payload._old_table = self.table_obj_no_uk
        self.payload._new_table = self.table_obj_1pk
        self.payload.allow_new_pk = True
        self.payload.decide_pk_for_filter()
        self.assertEquals(self.payload._pk_for_filter, ["id1", "id2", "id3"])
        self.assertTrue(self.payload.is_full_table_dump)

    def test_decide_pk_for_filter_newcol_not_indexed(self):
        # Old has PK and no prefix cols, should chunk
        self.payload._old_table = self.table_obj_1pk
        self.payload._new_table = self.table_obj_newcol
        self.payload.decide_pk_for_filter()

        self.assertEquals(self.payload._pk_for_filter, ["id1"])
        self.assertFalse(self.payload.is_full_table_dump)
        self.assertEqual(self.payload.find_coverage_index(), "PRIMARY")
        self.assertTrue(self.payload.validate_post_alter_pk())

        # (That new table has idx on added col is a NOP)
        self.payload._new_table = self.table_obj_newcol_with_idx
        self.payload.decide_pk_for_filter()

        self.assertEquals(self.payload._pk_for_filter, ["id1"])
        self.assertFalse(self.payload.is_full_table_dump)
        self.assertEqual(self.payload.find_coverage_index(), "PRIMARY")
        self.assertTrue(self.payload.validate_post_alter_pk())

    def test_decide_pk_for_filter_prefixed(self):
        # PK on old table uses prefixed columns, should NOT chunk.
        self.payload._old_table = self.table_obj_pk1_prefixed
        self.payload._new_table = self.table_obj_pk2_prefixed
        self.payload.decide_pk_for_filter()

        self.assertEquals(self.payload._pk_for_filter, ["id1", "name", "dummy1"])
        self.assertTrue(self.payload.is_full_table_dump)
        self.assertIsNone(self.payload.find_coverage_index())
        self.assertFalse(self.payload.validate_post_alter_pk())

    def test_decide_pk_for_filter_subset(self):
        self.payload._old_table = self.table_obj_2pk
        self.payload._new_table = self.table_obj_1pk
        self.payload.decide_pk_for_filter()

        # Going from 2 PK columns into 1 is considered as efficient
        self.assertEquals(self.payload._pk_for_filter, ["id1", "id2"])
        self.assertFalse(self.payload.is_full_table_dump)
        self.assertTrue(self.payload.validate_post_alter_pk())

    def test_use_sql_wsenv(self):
        with self.assertRaises(OSCError):
            self.payload = CopyPayload(use_sql_wsenv=True)

        self.payload = CopyPayload(use_sql_wsenv=True, outfile_dir="/a/b/c/")

        with self.assertRaises(OSCError):
            self.payload = CopyPayload(
                use_sql_wsenv=True, outfile_dir="a/b/c", skip_disk_space_check=False
            )
