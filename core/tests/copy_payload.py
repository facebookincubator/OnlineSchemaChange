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

import MySQLdb
import unittest
from ..lib.payload.copy import CopyPayload
from ..lib.payload.cleanup import CleanupPayload
from ..lib.sqlparse import parse_create
from ..lib.error import OSCError
from ..lib.mysql_version import MySQLVersion
from ..lib import constant
from mock import Mock


class CopyPayloadTestCase(unittest.TestCase):
    def payload_setup(self):
        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key ) ")
        payload._old_table = table_obj
        payload._new_table = table_obj
        payload._current_db = 'test'
        payload.range_start_vars_array = ['@ID']
        payload.range_end_vars_array = ['@ID']
        return payload

    def test_checksum_running_with_proper_idx(self):
        payload = CopyPayload()
        payload._new_table = Mock(indexes=[])
        payload._old_table = Mock(indexes=[])
        pri_key_list = []
        for i in range(3):
            col = Mock()
            col.name = 'col{}'.format(i)
            pri_key_list.append(col)

        payload._old_table.primary_key = Mock(is_unique=True)
        payload._old_table.primary_key.name = 'PRIMARY'
        payload._old_table.primary_key.column_list = pri_key_list

        payload._new_table.primary_key = Mock(is_unique=True)
        payload._new_table.primary_key.name = 'PRIMARY'
        payload._new_table.primary_key.column_list = pri_key_list

        # If primary key hasn't been changed, we can use that one for checksum
        payload._pk_for_filter = [c.name for c in pri_key_list]
        self.assertEqual(payload.find_coverage_index(), 'PRIMARY')

        # If new primary key has its left most prefix covering the old primary
        # key, we can use that as well
        col = Mock()
        col.name = 'col4'
        pri_key_list.append(col)
        payload._new_table.primary_key.column_list = pri_key_list
        self.assertEqual(payload.find_coverage_index(), 'PRIMARY')

        # If new primary key has its left most prefix covering the old primary
        # key, but the sequence is different, then we cannot use that
        pri_key_list = []
        for i in range(2, -1, -1):
            col = Mock()
            col.name = 'col{}'.format(i)
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
        payload.query = Mock(
            return_value=delta)
        # We will get whatever gap returned from MySQL database
        self.assertEqual(payload.get_gap_changes(), delta)
        # No more missing points after replay
        self.assertEqual(payload._replayed_chg_ids.missing_points(), [])

    def test_set_innodb_tmpdir(self):
        """
        Make sure set_innodb_tmpdir will catch and only catch 1231 error
        """

        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key ) ")
        payload._old_table = table_obj
        payload._new_table = table_obj

        payload.replay_changes = Mock()
        payload.execute_sql = Mock(
            side_effect=MySQLdb.OperationalError(1231, 'abc'))

        # Call the function make sure it catch the 1231 error
        payload.set_innodb_tmpdir('mock/path')

        # Call the function make sure it will still raise anything other than
        # 1231
        with self.assertRaises(MySQLdb.OperationalError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1111, 'abc'))
            payload.set_innodb_tmpdir('mock/path')
        self.assertEqual(err_context.exception.args[0], 1111)

    def test_long_select_being_killed(self):

        payload = self.payload_setup()
        query_id = 123
        payload.get_running_queries = Mock(return_value=[
            {'Info': b'SELECT 1 from a', 'db': 'test', 'Id': query_id}
        ])
        payload.execute_sql = Mock()
        payload.kill_query_by_id = Mock()
        # Try lock table, and make sure kill select will be called
        payload.lock_tables(tables=['a'])
        payload.kill_query_by_id.assert_called_with(query_id)

        # select in information_schema should be ignored
        payload.get_running_queries = Mock(return_value=[
            {'Info': b'SELECT 1 from a', 'db': 'information_schema',
             'Id': query_id}
        ])
        payload.execute_sql = Mock()
        payload.kill_query_by_id = Mock()
        # Try lock table, and make sure kill select will be called
        payload.lock_tables(tables=['a'])
        payload.kill_query_by_id.assert_not_called()

    def test_set_rocksdb_bulk_load(self):
        payload = CopyPayload()
        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key ) ENGINE=ROCKSDB")
        payload._old_table = table_obj
        payload._new_table = table_obj
        payload.execute_sql = Mock()
        payload.change_rocksdb_bulk_load()
        self.assertTrue(payload.execute_sql.called)

        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key ) ENGINE=ROCKSDB")
        new_table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int, id2 int, "
            "primary key (ID,id2)) ENGINE=ROCKSDB")
        payload._old_table = table_obj
        payload._new_table = new_table_obj
        payload.execute_sql = Mock()
        payload.change_rocksdb_bulk_load()
        self.assertFalse(payload.execute_sql.called)

        table_obj = parse_create(
            " CREATE TABLE a "
            "( ID int primary key ) ENGINE=ROCKSDB")
        payload._old_table = table_obj
        payload._new_table = table_obj

        payload.execute_sql = Mock(
            side_effect=MySQLdb.OperationalError(1193, 'abc'))
        payload.change_rocksdb_bulk_load()

    def test_skip_cleanup(self):
        payload = CopyPayload()
        sql = 'CREATE TABLE abc (ID int)'
        database = 'db'
        payload._old_table = Mock()
        payload._old_table.name = 'abc'

        # add some drop table entry pretending we've done some work
        payload._cleanup_payload = CleanupPayload(db=database)
        payload._cleanup_payload.add_drop_table_entry(
            database,
            constant.DELTA_TABLE_PREFIX + 'abc')
        payload._cleanup_payload.add_drop_table_entry(
            database,
            constant.NEW_TABLE_PREFIX + 'abc')
        payload._cleanup_payload.cleanup = Mock()

        # If we don't skip cleanup, then we should have 2 tables to clean up
        payload.skip_cleanup_after_kill = False
        with self.assertRaises(OSCError) as err_context:
            payload.init_connection = Mock(
                side_effect=MySQLdb.OperationalError(
                    2006, 'MySQL has gone away'))
            payload.run_ddl(database, sql)
        self.assertEqual(len(payload._cleanup_payload.to_drop), 2)
        self.assertEqual(err_context.exception.err_key, 'GENERIC_MYSQL_ERROR')

        # If we are skipping cleanup, then there's nothing to cleanup
        payload.skip_cleanup_after_kill = True
        with self.assertRaises(OSCError) as err_context:
            payload.init_connection = Mock(
                side_effect=MySQLdb.OperationalError(
                    2006, 'MySQL has gone away'))
            payload.run_ddl(database, sql)
        # There should be no cleanup entry at all if we skip the table cleanup
        self.assertEqual(payload._cleanup_payload.to_drop, [])
        self.assertEqual(err_context.exception.err_key, 'GENERIC_MYSQL_ERROR')

    def test_file_exists(self):
        payload = self.payload_setup()
        with self.assertRaises(OSCError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1086, 'abc'))
            payload.select_full_table_into_outfile()
        self.assertEqual(err_context.exception.err_key, 'FILE_ALREADY_EXIST')

        with self.assertRaises(OSCError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1086, 'abc'))
            payload.select_chunk_into_outfile('path/to/outfile', False)
        self.assertEqual(err_context.exception.err_key, 'FILE_ALREADY_EXIST')

        # Any mysql error other than 1086 should surface
        with self.assertRaises(MySQLdb.OperationalError) as err_context:
            payload.execute_sql = Mock(
                side_effect=MySQLdb.OperationalError(1111, 'abc'))
            payload.select_chunk_into_outfile('path/to/outfile', False)
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
        partitions = ['p1', 'p2', 'p3']

        # No difference between old and new
        payload.query = Mock(return_value=None)
        # No-op for create table
        payload.execute_sql = Mock()
        payload.fetch_partitions = Mock(return_value=partitions)
        # We will get whatever gap returned from MySQL database
        payload._cleanup_payload.add_drop_table_entry = Mock()
        payload.create_copy_table()
        payload._cleanup_payload.add_drop_table_entry.assert_called_with(
            payload._current_db, payload.new_table_name, partitions)

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
            " CREATE TABLE a "
            "( id1 int ,  "
            "col1 varchar(10), "
            "PRIMARY KEY (id1))"
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        # No change in the schema
        self.assertEqual(
            payload.dropped_column_name_list,
            []
        )

        payload._new_table = table_obj_pri_dropped
        self.assertEqual(
            payload.dropped_column_name_list,
            ['id2']
        )

        payload._new_table = table_obj_col_dropped
        self.assertEqual(
            payload.dropped_column_name_list,
            ['col2']
        )

        payload._new_table = table_obj_both_dropped
        self.assertEqual(
            payload.dropped_column_name_list,
            ['id2', 'col2']
        )

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
            " CREATE TABLE a "
            "( ID int primary key,  "
            "col2 varchar(100)) "
        )
        payload._old_table = table_obj
        payload._new_table = table_obj
        # No change in the schema
        self.assertEqual(
            payload.checksum_column_list,
            ['col1', 'col2']
        )

        # changed column being kept
        payload._new_table = table_obj_new
        payload.skip_checksum_for_modified = False
        self.assertEqual(
            payload.checksum_column_list,
            ['col1', 'col2']
        )

        # skip changed
        payload._new_table = table_obj_new
        payload.skip_checksum_for_modified = True
        self.assertEqual(
            payload.checksum_column_list,
            ['col1']
        )

        # skip dropped
        payload._new_table = table_obj_dropped
        payload.skip_checksum_for_modified = False
        self.assertEqual(
            payload.checksum_column_list,
            ['col2']
        )

        # skip dropped
        payload._new_table = table_obj_dropped
        payload.skip_checksum_for_modified = False
        self.assertEqual(
            payload.checksum_column_list,
            ['col2']
        )

    def test_parse_session_overrides_str_empty(self):
        payload = self.payload_setup()
        overrides_str = ''
        expected_overrides = []
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_num(self):
        payload = self.payload_setup()
        overrides_str = 'var1=1'
        expected_overrides = [['var1', '1']]
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_str(self):
        payload = self.payload_setup()
        overrides_str = 'var1=v'
        expected_overrides = [['var1', 'v']]
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_list(self):
        payload = self.payload_setup()
        overrides_str = 'var1=v;var2=1'
        expected_overrides = [['var1', 'v'], ['var2', '1']]
        overrides = payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(overrides, expected_overrides)

    def test_parse_session_overrides_str_malform(self):
        payload = self.payload_setup()
        overrides_str = 'var1=v;var2='
        with self.assertRaises(OSCError) as err_context:
            payload.parse_session_overrides_str(overrides_str)
        self.assertEqual(err_context.exception.err_key,
                         'INCORRECT_SESSION_OVERRIDE')

    def test_execute_sql_not_called_for_empty_overrides(self):
        # we shouldn't execute any sql if there's no session overrides
        payload = self.payload_setup()
        payload.execute_sql = Mock()
        payload.session_overrides_str = ''
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
        self.assertEqual(err_context.exception.err_key,
                         'REPLAY_WRONG_AFFECTED')

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
        payload.mysql_vars['binlog_format'] = 'STATEMENT'
        payload.mysql_version = MySQLVersion('5.1.1')
        self.assertTrue(payload.is_trigger_rbr_safe)

    def test_is_rbr_safe_row_fb(self):
        # is_trigger_rbr_safe should always be True if Facebook version
        # is being used and sql_log_bin_triggers is OFF
        payload = self.payload_setup()
        payload.mysql_vars['binlog_format'] = 'ROW'
        payload.mysql_vars['sql_log_bin_triggers'] = 'OFF'
        payload.mysql_version = MySQLVersion('5.1.1-fb')
        self.assertTrue(payload.is_trigger_rbr_safe)

    def test_is_rbr_safe_row_fb_but_logs_on(self):
        # is_trigger_rbr_safe should False if we are using a Facebook version
        # but sql_log_bin_triggers is still enabled
        payload = self.payload_setup()
        payload.mysql_vars['binlog_format'] = 'ROW'
        payload.mysql_vars['sql_log_bin_triggers'] = 'ON'
        payload.mysql_version = MySQLVersion('5.1.1-fb')
        self.assertFalse(payload.is_trigger_rbr_safe)

    def test_is_rbr_safe_row_other_forks(self):
        # is_trigger_rbr_safe should False if we are using a Facebook version
        # but sql_log_bin_triggers is still enabled
        payload = self.payload_setup()
        payload.mysql_vars['binlog_format'] = 'ROW'
        payload.mysql_version = MySQLVersion('5.5.30-percona')
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
            groups, [
                (1, [1]),
                (2, [2]),
                (3, [3]),
                (1, [4]),
                (2, [5]),
                (3, [6]),
            ])

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
            groups, [
                (1, [1]),
                (2, [2, 3, 4]),
                (1, [5]),
            ])

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
            groups, [
                (1, [1]),
                (3, [2]),
                (3, [3]),
                (3, [4]),
                (1, [5]),
            ])

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
            groups, [
                (1, [1]),
                (2, [2, 3]),
                (2, [4]),
                (1, [5]),
            ])
