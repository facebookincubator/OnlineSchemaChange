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

import glob
import logging
import os
import MySQLdb
import time
from copy import deepcopy

from .base import Payload
from .cleanup import CleanupPayload
from .. import constant
from .. import sql
from .. import util
from ..error import OSCError
from ..hook import wrap_hook
from ..mysql_version import MySQLVersion
from ..sqlparse import parse_create, ParseError, is_equal

log = logging.getLogger(__name__)


class CopyPayload(Payload):
    """
    This payload implements the actual OSC logic. Basically it'll create a new
    physical table and then load data into it while it keeps the original table
    serving read/write requests. Later it will replay the changes captured
    by trigger onto the new table. Finally, a table name flip will be
    issued to make the new schema serve requests

    Properties in this class have consistant name convention. A property name
    will look like:
        [old/new]_[pk/non_pk]_column_list
    with:
        - old/new representing which schema these columns are from, old or new
        - pk/non_pk representing whether these columns are a part of primary
            key
    """
    IDCOLNAME = '_osc_ID_'
    DMLCOLNAME = '_osc_dml_type_'

    DML_TYPE_INSERT = 1
    DML_TYPE_DELETE = 2
    DML_TYPE_UPDATE = 3

    def __init__(self, *args, **kwargs):
        super(CopyPayload, self).__init__(*args, **kwargs)
        self._current_db = None
        self._pk_for_filter = []
        self.mysql_vars = {}
        self._idx_name_for_filter = 'PRIMARY'
        self._new_table = None
        self._old_table = None
        self._replayed_chg_ids = util.RangeChain()
        self.select_chunk_size = 0
        self.bypass_replay_timeout = False
        self.is_slave_stopped_by_me = False
        self.stop_before_swap = False
        self.is_skip_fcache_supported = False
        self.outfile_suffix_end = 0
        self.last_replayed_id = 0
        self.last_checksumed_id = 0
        self.table_size = 0
        self.session_overrides = []
        self._cleanup_payload = CleanupPayload(*args, **kwargs)
        self.stats = {}
        self.partitions = {}
        self.eta_chunks = 1

        self.repl_status = kwargs.get('repl_status', '')
        self.outfile_dir = kwargs.get('outfile_dir', '')
        # By specify this option we are allowed to open a long transaction
        # during full table dump and full table checksum
        self.allow_new_pk = kwargs.get('allow_new_pk', False)
        self.allow_drop_column = kwargs.get('allow_drop_column', False)
        self.detailed_mismatch_info = kwargs.get(
            'detailed_mismatch_info', False)
        self.dump_after_checksum = kwargs.get(
            'dump_after_checksum', False)
        self.eliminate_dups = kwargs.get('eliminate_dups', False)
        self.rm_partition = kwargs.get('rm_partition', False)
        self.force_cleanup = kwargs.get('force_cleanup', False)
        self.skip_cleanup_after_kill = kwargs.get(
            'skip_cleanup_after_kill', False)
        self.pre_load_statement = kwargs.get('pre_load_statement', '')
        self.post_load_statement = kwargs.get('post_load_statement', '')
        self.replay_max_attempt = kwargs.get(
            'replay_max_attempt', constant.DEFAULT_REPLAY_ATTEMPT)
        self.replay_timeout = kwargs.get(
            'replay_timeout', constant.REPLAY_DEFAULT_TIMEOUT)
        self.replay_batch_size = kwargs.get(
            'replay_batch_size', constant.DEFAULT_BATCH_SIZE)
        self.replay_group_size = kwargs.get(
            'replay_group_size', constant.DEFAULT_REPLAY_GROUP_SIZE)
        self.skip_pk_coverage_check = kwargs.get(
            'skip_pk_coverage_check', False)
        self.skip_long_trx_check = kwargs.get(
            'skip_long_trx_check', False)
        self.ddl_file_list = kwargs.get('ddl_file_list', '')
        self.free_space_reserved = kwargs.get(
            'free_space_reserved', constant.DEFAULT_RESERVED_SPACE)
        self.chunk_size = kwargs.get(
            'chunk_size', constant.CHUNK_BYTES)
        self.long_trx_time = kwargs.get(
            'long_trx_time', constant.LONG_TRX_TIME)
        self.max_running_before_ddl = kwargs.get(
            'max_running_before_ddl', constant.MAX_RUNNING_BEFORE_DDL)
        self.ddl_guard_attempts = kwargs.get(
            'ddl_guard_attempts', constant.DDL_GUARD_ATTEMPTS)
        self.lock_max_attempts = kwargs.get(
            'lock_max_attempts', constant.LOCK_MAX_ATTEMPTS)
        self.session_timeout = kwargs.get(
            'mysql_session_timeout', constant.SESSION_TIMEOUT)
        self.idx_recreation = kwargs.get(
            'idx_recreation', False)
        self.rebuild = kwargs.get('rebuild', False)
        self.keep_tmp_table = kwargs.get(
            'keep_tmp_table_after_exception', False)
        self.skip_checksum = kwargs.get('skip_checksum', False)
        self.skip_checksum_for_modified = kwargs.get(
            'skip_checksum_for_modified', False)
        self.skip_named_lock = kwargs.get(
            'skip_named_lock', False)
        self.skip_affected_rows_check = kwargs.get(
            'skip_affected_rows_check', False)
        self.where = kwargs.get('where', None)
        self.session_overrides_str = kwargs.get(
            'session_overrides', '')
        self.is_full_table_dump = False

    @property
    def current_db(self):
        """
        The database name this payload currently working on
        """
        return self._current_db

    @property
    def old_pk_list(self):
        """
        List of column names representing the primary key in
        the old schema.
        If will be used to check whether the old schema has a primary key by
        comparing the length to zero. Also will be used in construct the
        condition part of the replay query
        """
        return [
            col.name
            for col in self._old_table.primary_key.column_list
        ]

    @property
    def dropped_column_name_list(self):
        """
        list of column names which exists only in old schema
        """
        column_list = []
        new_tbl_columns = [col.name for col in self._new_table.column_list]
        for col in self._old_table.column_list:
            if col.name not in new_tbl_columns:
                column_list.append(col.name)
        return column_list

    @property
    def old_column_list(self):
        """
        list of column names for all the columns in the old schema except
        the ones are being dropped in the new schema.
        It will be used in query construction for checksum
        """
        return [
            col.name
            for col in self._old_table.column_list
            if col.name not in self.dropped_column_name_list
        ]

    @property
    def old_non_pk_column_list(self):
        """
        A list of column name for all non-pk columns in
        the old schema. It will be used in query construction for replay
        """
        return [
            col.name for col in self._old_table.column_list
            if col.name not in self._pk_for_filter and
            col.name not in self.dropped_column_name_list
        ]

    @property
    def checksum_column_list(self):
        """
        A list of non-pk column name suitable for comparing checksum
        """
        column_list = []
        old_pk_name_list = [
            c.name for c in self._old_table.primary_key.column_list]
        for col in self._old_table.column_list:
            if col.name in old_pk_name_list:
                continue
            if col.name in self.dropped_column_name_list:
                continue
            new_columns = {
                col.name: col for col in self._new_table.column_list}
            if col != new_columns[col.name]:
                if self.skip_checksum_for_modified:
                    continue
            column_list.append(col.name)
        return column_list

    @property
    def delta_table_name(self):
        """
        Name of the physical intermediate table for data loading. Used almost
        everywhere
        """
        return constant.DELTA_TABLE_PREFIX + self._old_table.name

    @property
    def table_name(self):
        """
        Name of the original table. Because we don't support table name change
        in OSC, name of the existing table should be the exactly the same as
        the one in the sql file.
        We are using 'self._new_table.name' here instead of _old_table, because
        _new_table will be instantiated before _old_table at early stage.
        It will be used by some sanity checks before we fetching data from
        information_schema
        """
        return self._new_table.name

    @property
    def new_table_name(self):
        """
        Name of the physical temporary table for loading data during OSC
        """
        return constant.NEW_TABLE_PREFIX + self.table_name

    @property
    def renamed_table_name(self):
        """
        Name of the old table after swap.
        """
        return constant.RENAMED_TABLE_PREFIX + self._old_table.name

    @property
    def insert_trigger_name(self):
        """
        Name of the "AFTER INSERT" trigger on the old table to capture changes
        during data dump/load
        """
        return constant.INSERT_TRIGGER_PREFIX + self._old_table.name

    @property
    def update_trigger_name(self):
        """
        Name of the "AFTER UPDATE" trigger on the old table to capture changes
        during data dump/load
        """
        return constant.UPDATE_TRIGGER_PREFIX + self._old_table.name

    @property
    def delete_trigger_name(self):
        """
        Name of the "AFTER DELETE" trigger on the old table to capture changes
        during data dump/load
        """
        return constant.DELETE_TRIGGER_PREFIX + self._old_table.name

    @property
    def outfile(self):
        """
        Full file path of the outfile for data dumping/loading. It's the prefix
        of outfile chunks. A single outfile chunk will look like
        '@datadir/__osc_tbl_@TABLE_NAME.1'
        """
        return os.path.join(self.outfile_dir,
                            constant.OUTFILE_TABLE + self.table_name)

    @property
    def tmp_table_exclude_id(self):
        """
        Name of the temporary table which contains the value of IDCOLNAME in
        self.delta_table_name which we've already replayed
        """
        return '__osc_temp_ids_to_exclude'

    @property
    def tmp_table_include_id(self):
        """
        Name of the temporary table which contains the value of IDCOLNAME in
        self.delta_table_name which we will be replaying for a single
        self.replay_changes() call
        """
        return '__osc_temp_ids_to_include'

    @property
    def outfile_exclude_id(self):
        """
        Name of the outfile which contains the data which will be loaded to
        self.tmp_table_exclude_id soon. We cannot use insert into select
        from, because that will hold gap lock inside transaction. The whole
        select into outfile/load data infile logic is a work around for this.
        """
        return os.path.join(self.outfile_dir,
                            constant.OUTFILE_EXCLUDE_ID + self.table_name)

    @property
    def outfile_include_id(self):
        """
        Name of the outfile which contains the data which will be loaded to
        self.tmp_table_include_id soon. See docs in self.outfile_exclude_id
        for more
        """
        return os.path.join(self.outfile_dir,
                            constant.OUTFILE_INCLUDE_ID + self.table_name)

    @property
    def droppable_indexes(self):
        """
        A list of lib.sqlparse.models objects representing the indexes which
        can be dropped before loading data into self.new_table_name to speed
        up data loading
        """
        # If we don't specified index recreation then just return a empty list
        # which stands for no index is suitable of dropping
        if not self.idx_recreation:
            return []
        # We need to keep unique index, if we need to use it to eliminate
        # duplicates during data loading
        return self._new_table.droppable_indexes(
            keep_unique_key=self.eliminate_dups)

    def set_tx_isolation(self):
        """
        Setting the session isolation level to RR for OSC
        """
        self.execute_sql(
            sql.set_session_variable('tx_isolation'), ('REPEATABLE-READ',))

    def set_sql_mode(self):
        """
        Setting the sql_mode to STRICT for the connection we will using for OSC
        """
        self.execute_sql(
            sql.set_session_variable('sql_mode'), ('STRICT_ALL_TABLES',))

    def parse_session_overrides_str(self, overrides_str):
        """
        Given a session overrides string, break it down to a list of overrides

        @param overrides_str:  A plain string that contains the overrides
        @type  overrides_str:  string

        @return : A list of [var, value]
        """
        overrides = []
        if overrides_str is None or overrides_str == '':
            return []
        for section in overrides_str.split(';'):
            splitted_array = section.split('=')
            if len(splitted_array) != 2 or splitted_array[0] == '' or \
                    splitted_array[1] == '':
                raise OSCError('INCORRECT_SESSION_OVERRIDE',
                               {'section': section})
            overrides.append(splitted_array)
        return overrides

    def override_session_vars(self):
        """
        Override session variable if there's any
        """
        self.session_overrides = self.parse_session_overrides_str(
            self.session_overrides_str)
        for var_name, var_value in self.session_overrides:
            log.info("Override session variable {} with value: {}"
                     .format(var_name, var_value))
            self.execute_sql(
                sql.set_session_variable(var_name), (var_value,))

    def get_mysql_settings(self):
        result = self.query(sql.show_variables)
        for row in result:
            self.mysql_vars[row['Variable_name']] = row['Value']

    def init_mysql_version(self):
        """
        Parse the mysql_version string into a version object
        """
        self.mysql_version = MySQLVersion(self.mysql_vars['version'])

    def is_var_enabled(self, var_name):
        if var_name not in self.mysql_vars:
            return False
        if self.mysql_vars[var_name] == 'OFF':
            return False
        if self.mysql_vars[var_name] == '0':
            return False
        return True

    @property
    def is_trigger_rbr_safe(self):
        """
        Only fb-mysql is safe for RBR if we create trigger on master alone
        Otherwise slave will hit _chg table not exists error
        """
        # We only need to check this if RBR is enabled
        if self.mysql_vars['binlog_format'] == 'ROW':
            if self.mysql_version.is_fb:
                if not self.is_var_enabled('sql_log_bin_triggers'):
                    return True
                else:
                    return False
            else:
                return False
        else:
            return True

    def sanity_checks(self):
        """
        Check MySQL setting for requirements that we don't necessarily need to
        hold a name lock for
        """
        if not self.is_trigger_rbr_safe:
            raise OSCError('NOT_RBR_SAFE')

    @wrap_hook
    def init_connection(self, db):
        """
        Initiate a connection for OSC, set session variables and get OSC lock
        This connection will be the only connection for the whole OSC operation
        It also maintain some internal state using MySQL temporary table. So
        an interrupted connection means a failure for the whole OSC attempt.
        """
        log.info("== Stage 1: Init ==")
        self.use_db(db)
        self.set_no_binlog()
        self.get_mysql_settings()
        self.init_mysql_version()
        self.sanity_checks()
        self.set_tx_isolation()
        self.set_sql_mode()
        self.override_session_vars()
        self.get_osc_lock()

    def table_exists(self, table_name):
        """
        Given a table_name check whether this table already exist under
        current working database

        @param table_name:  Name of the table to check existence
        @type  table_name:  string
        """
        table_exists = self.query(sql.table_existence,
                                  (table_name, self._current_db,))
        return bool(table_exists)

    def fetch_old_table(self):
        """
        Use lib.sqlparse.parse_create to turn a CREATE TABLE syntax into a
        TABLE object, so that we can then do stuffs in a phythonic way later
        """
        ddl = self.query(sql.show_create_table(self.table_name))
        if ddl:
            try:
                self._old_table = parse_create(ddl[0]['Create Table'])
            except ParseError as e:
                raise OSCError(
                    'TABLE_PARSING_ERROR',
                    {'db': self._current_db, 'table': self.table_name,
                     'msg': str(e)})

    def fetch_partitions(self, table_name):
        """
        Fetching partition names from information_schema. This will be used
        when dropping table. If a table has a partition schema, then its
        partition will be dropped one by one before the table get dropped.
        This way we will bring less pressure to the MySQL server
        """
        partition_result = self.query(sql.fetch_partition,
                                      (self._current_db, table_name,))
        # If a table doesn't have partition schema the "PARTITION_NAME"
        # will be string "None" instead of something considered as false
        # in python
        return [partition_entry['PARTITION_NAME']
                for partition_entry in partition_result
                if partition_entry['PARTITION_NAME'] != 'None']

    @wrap_hook
    def init_table_obj(self):
        """
        Instantiate self._old_table by parsing the output of SHOW CREATE
        TABLE from MySQL instance. Because we need to parse out the table name
        we'll act on, this should be the first step before we start to doing
        anything
        """
        # Check the existence of original table
        if not self.table_exists(self.table_name):
            raise OSCError('TABLE_NOT_EXIST',
                           {'db': self._current_db, 'table': self.table_name})
        self.fetch_old_table()
        self.partitions[self.table_name] = self.fetch_partitions(
            self.table_name)
        # The table after swap will have the same partition layout as current
        # table
        self.partitions[self.renamed_table_name] = \
            self.partitions[self.table_name]
        # Preserve the auto_inc value from old table, so that we don't revert
        # back to a smaller value after OSC
        if self._old_table.auto_increment:
            self._new_table.auto_increment = self._old_table.auto_increment

    def cleanup_with_force(self):
        """
        Loop through all the tables we will touch during OSC, and clean them
        up if force_cleanup is specified
        """
        log.info("--force-cleanup specified, cleaning up things that may left "
                 "behind by last run")
        cleanup_payload = CleanupPayload(charset=self.charset, sudo=self.sudo)
        # cleanup outfiles for include_id and exclude_id
        for filepath in (
                self.outfile_exclude_id, self.outfile_include_id):
            cleanup_payload.add_file_entry(filepath)
        # cleanup outfiles for detailed checksum
        for suffix in ['old', 'new']:
            cleanup_payload.add_file_entry(
                "{}.{}".format(self.outfile, suffix))
        # cleanup outfiles for table dump
        file_prefixes = [
            self.outfile,
            "{}.old".format(self.outfile),
            "{}.new".format(self.outfile)]
        for file_prefix in file_prefixes:
            log.debug("globbing {}".format(file_prefix))
            for outfile in glob.glob("{}.[0-9]*".format(file_prefix)):
                cleanup_payload.add_file_entry(outfile)
        for trigger in (
                self.delete_trigger_name, self.update_trigger_name,
                self.insert_trigger_name):
            cleanup_payload.add_drop_trigger_entry(self._current_db, trigger)
        for tbl in (
                self.new_table_name, self.delta_table_name,
                self.renamed_table_name):
            partitions = self.fetch_partitions(tbl)
            cleanup_payload.add_drop_table_entry(
                self._current_db, tbl, partitions)
        cleanup_payload.mysql_user = self.mysql_user
        cleanup_payload.mysql_pass = self.mysql_pass
        cleanup_payload.socket = self.socket
        cleanup_payload.get_conn_func = self.get_conn_func
        cleanup_payload.cleanup(self._current_db)
        cleanup_payload.close_conn()

    @wrap_hook
    def determine_outfile_dir(self):
        """
        Determine the output directory we will use to store dump file
        """
        if self.outfile_dir:
            return
        # if --tmpdir is not specified on command line for outfiles
        # use @@secure_file_priv
        for var_name in ('@@secure_file_priv', '@@datadir'):
            result = self.query(
                sql.select_as(var_name, 'folder'))
            if not result:
                raise Exception('Failed to get {} system variable'
                                .format(var_name))
            if result[0]['folder']:
                if var_name == '@@secure_file_priv':
                    self.outfile_dir = result[0]['folder']
                else:
                    self.outfile_dir = os.path.join(
                        result[0]['folder'],
                        self._current_db)
                log.info("Will use {} storing dump outfile"
                         .format(self.outfile_dir))
                return
        raise Exception('Cannot determine output dir for dump')

    def trigger_check(self):
        """
        Check whether there's any trigger already exist on the table we're
        about to touch
        """
        triggers = self.query(
            sql.trigger_existence,
            (self.table_name, self._current_db),)
        if triggers:
            trigger_desc = []
            for trigger in triggers:
                trigger_desc.append(
                    "Trigger name: {}, Action: {} {}"
                    .format(trigger['TRIGGER_NAME'],
                            trigger['ACTION_TIMING'],
                            trigger['EVENT_MANIPULATION']))
            raise OSCError("TRIGGER_ALREADY_EXIST",
                           {'triggers': "\n".join(trigger_desc)})

    def foreign_key_check(self):
        """
        Check whether the table has been referred to any existing foreign
        definition
        """
        foreign_keys = self.query(
            sql.foreign_key_cnt,
            (self.table_name, self._current_db,
             self.table_name, self._current_db,))
        if foreign_keys and foreign_keys[0]['count'] > 0:
            raise OSCError("FOREIGN_KEY_FOUND",
                           {'db': self._current_db,
                            'table': self.table_name})

    def get_table_size(self, table_name):
        """
        Given a table_name return its current size in Bytes

        @param table_name:  Name of the table to fetch size
        @type  table_name:  string
        """
        result = self.query(sql.show_table_stats(self._current_db),
                            (self.table_name,))
        if result:
            return result[0]['Data_length'] + result[0]['Index_length']
        return 0

    def check_disk_size(self):
        """
        Check if we have enough disk space to execute the DDL
        """
        self.table_size = self.get_table_size(self.table_name)
        disk_space = util.spare_disk_size(self.outfile_dir)
        # With allow_new_pk, we will create one giant outfile, and so at
        # some point will have the entire new table and the entire outfile
        # both existing simultaneously.
        if self.allow_new_pk and not self._old_table.primary_key.column_list:
            required_size = self.table_size * 2
        else:
            required_size = self.table_size * 1.1
        log.info("Disk space required: {}, available: {}"
                 .format(util.readable_size(required_size),
                         util.readable_size(disk_space)))
        if required_size > disk_space:
            raise OSCError('NOT_ENOUGH_SPACE',
                           {'need': util.readable_size(required_size),
                            'avail': util.readable_size(disk_space)})

    def validate_post_alter_pk(self):
        """
        As we force (primary) when replaying changes, we have to make sure
        rows in new table schema can be accessed using old PK combination.
        The logic here is to make sure the old table's primary key list equals
        to the set which one of the new table's index prefix can form.
        Otherwise there'll be a performance issue when replaying changes
        based on old primary key combination.
        Note that if old PK is (a, b), new PK is (b, a, c) is acceptable,
        because for each combination of (a, b), it still can utilize the new
        PK for row searching
        However if the old PK is (a, b, c), new PK is (b, c, d). Then there's
        a chance the changes may not be able to be replay efficiently. Because
        using only column (b, c) for row searching may result in a huge number
        of matched rows
        """
        idx_on_new_table = [self._new_table.primary_key] + \
            self._new_table.indexes
        old_pk_len = len(self._pk_for_filter)
        for idx in idx_on_new_table:
            log.debug("Checking prefix for {}".format(idx.name))
            idx_prefix = idx.column_list[:old_pk_len]
            idx_name_set = {col.name for col in idx_prefix}
            if set(self._pk_for_filter) == idx_name_set:
                log.info("PK prefix on new table can cover PK from old table")
                return True
        return False

    def find_coverage_index(self):
        """
        Find an unique index which can perfectly cover old pri-key search in
        order to calculate checksum for new table. We will use this index name
        as force index in checksum query
        See validate_post_alter_pk for more detail about pri-key coverage
        """
        idx_on_new_table = [self._new_table.primary_key] + \
            self._new_table.indexes
        old_pk_len = len(self._pk_for_filter)
        for idx in idx_on_new_table:
            idx_prefix = idx.column_list[:old_pk_len]
            idx_name_list = [col.name for col in idx_prefix]
            if self._pk_for_filter == idx_name_list:
                if idx.is_unique:
                    return idx.name
        return None

    def init_range_variables(self):
        """
        Initial array and string which contains the same number of session
        variables as the columns of primary key.
        This will be used as chunk boundary when dumping and checksuming
        """
        self.range_start_vars_array = []
        self.range_end_vars_array = []

        for idx in range(len(self._pk_for_filter)):
            self.range_start_vars_array.append('@range_start_{}'.format(idx))
            self.range_end_vars_array.append('@range_end_{}'.format(idx))
        self.range_start_vars = ','.join(self.range_start_vars_array)
        self.range_end_vars = ','.join(self.range_end_vars_array)

    def make_chunk_size_odd(self):
        """
        Ensure select_chunk_size is an odd number. Because we use this number
        as chunk size for checksum as well. If a column has exact the same
        value for all its rows, then return value from BIT_XOR(CRC32(`col`))
        will be zero for even number of rows, no matter what value it has.
        """
        if self.select_chunk_size % 2 == 0:
            self.select_chunk_size = self.select_chunk_size + 1

    def get_table_chunk_size(self):
        """
        Calculate the number of rows for each table dump query table based on
        average row length and the chunks size we've specified
        """
        result = self.query(sql.table_avg_row_len,
                            (self._current_db, self.table_name,))
        if result:
            tbl_avg_length = result[0]['AVG_ROW_LENGTH']
            # avoid huge chunk row count
            if tbl_avg_length < 20:
                tbl_avg_length = 20
            self.select_chunk_size = self.chunk_size // tbl_avg_length
            # This means either the avg row size is huge, or user specified
            # a tiny select_chunk_size on CLI. Let's make it one row per outfile
            # to avoid zero division
            if not self.select_chunk_size:
                self.select_chunk_size = 1
            log.info("Outfile will contain {} rows each"
                     .format(self.select_chunk_size))
            self.eta_chunks = max(int(
                result[0]['TABLE_ROWS'] / self.select_chunk_size), 1)
        else:
            raise OSCError('FAIL_TO_GUESS_CHUNK_SIZE')

    def has_desired_schema(self):
        """
        Check whether the existing table already has the desired schema.
        """
        if self._new_table == self._old_table:
            if not self.rebuild:
                log.info("Table already has the desired schema. ")
                return True
            else:
                log.info("Table already has the desired schema. However "
                         "--rebuild is specified, doing a rebuild instead")
                return False
        return False

    @wrap_hook
    def pre_osc_check(self):
        """
        Pre-OSC sanity check.
        Make sure all temporary table which will be used during data copy
        stage doesn't exist before we actually creating one.
        Also doing some index sanity check.
        """
        # Make sure temporary table we will use during copy doesn't exist
        tables_to_check = (
            self.new_table_name,
            self.delta_table_name,
            self.renamed_table_name)
        for table_name in tables_to_check:
            if self.table_exists(table_name):
                raise OSCError('TABLE_ALREADY_EXIST',
                               {'db': self._current_db, 'table': table_name})

        # Make sure new table schema has primary key
        if not all((self._new_table.primary_key,
                    self._new_table.primary_key.column_list)):
            raise OSCError(
                'NO_PK_EXIST',
                {'db': self._current_db, 'table': self.table_name})

        # If we are adding a PK, then we should use all the columns in
        # old table to identify an unique row
        if not all((
                self._old_table.primary_key,
                self._old_table.primary_key.column_list)):
            # Let's try to get an UK if possible
            for idx in self._old_table.indexes:
                if idx.is_unique:
                    log.info("Old table doesn't have a PK but has an UK: {}"
                             .format(idx.name))
                    self._pk_for_filter = [
                        col.name for col in idx.column_list]
                    self._idx_name_for_filter = idx.name
                    break
            else:
                # There's no UK either
                if self.allow_new_pk:
                    self._pk_for_filter = [
                        col.name for col in self._old_table.column_list]
                    self.is_full_table_dump = True
                else:
                    raise OSCError('NEW_PK')
        # If we have PK in existing schema, then we use current PK as an unique
        # row finder
        else:
            self._pk_for_filter = [
                col.name for col in self._old_table.primary_key.column_list]

        # Check if we can have indexes in new table to efficiently look up
        # current old pk combinations
        if not self.validate_post_alter_pk():
            if self.skip_pk_coverage_check:
                log.warning(
                    "Indexes on new table cannot cover current PK of "
                    "the old schema, which will make binary logs replay "
                    "in an inefficient way.")
            elif self.is_full_table_dump:
                log.warning(
                    "Skipping coverage index test, since we are doing full "
                    "table dump")
            else:
                old_pk_names = ", ".join(
                    "`{}`".format(col.name)
                    for col in self._old_table.primary_key.column_list)
                raise OSCError('NO_INDEX_COVERAGE',
                               {'pk_names': old_pk_names})

        if self.check_no_fcache_support():
            self.is_skip_fcache_supported = True

        log.debug("PK filter for replyaing changes later: {}"
                  .format(self._pk_for_filter))

        self.foreign_key_check()
        self.trigger_check()
        self.init_range_variables()
        self.get_table_chunk_size()
        self.make_chunk_size_odd()
        self.check_disk_size()

    def add_drop_table_entry(self, table_name):
        """
        A wrapper for adding drop table request to CleanupPayload.
        The database name will always be the one we are currently working on.
        Also partition name list will be included as fetched from information
        schema before DDL
        """
        self._cleanup_payload.add_drop_table_entry(
            self._current_db, table_name, self.partitions.get(table_name, []))

    @wrap_hook
    def create_copy_table(self):
        """
        Create the physical temporary table using new schema
        """
        tmp_sql_obj = deepcopy(self._new_table)
        tmp_sql_obj.name = self.new_table_name
        if self.rm_partition:
            tmp_sql_obj.partition = self._old_table.partition
        tmp_table_ddl = tmp_sql_obj.to_sql()
        log.info("Creating copy table using: {}".format(tmp_table_ddl))
        self.execute_sql(tmp_table_ddl)
        table_diff = self.query(
            sql.column_diff,
            (self.new_table_name, self.table_name, self._current_db,))
        if table_diff:
            if self.allow_drop_column:
                for diff_column in table_diff:
                    log.warning("Column `{}` is missing in the new schema, "
                                "but --alow-drop-column is specified. Will "
                                "drop this column."
                                .format(diff_column['COLUMN_NAME']))
            else:
                missing_columns = ', '.join(
                    col['COLUMN_NAME'] for col in table_diff)
                raise OSCError('MISSING_COLUMN',
                               {'column': missing_columns})
            # We don't allow dropping columns from current primary key
            for col in self._pk_for_filter:
                if col in self.dropped_column_name_list:
                    raise OSCError('PRI_COL_DROPPED', {'pri_col': col})

        self.partitions[self.new_table_name] = self.fetch_partitions(
            self.new_table_name)
        self.add_drop_table_entry(self.new_table_name)

    @wrap_hook
    def create_delta_table(self):
        """
        Create the table which will store changes made to existing table during
        OSC. This can be considered as table level binlog
        """
        self.execute_sql(
            sql.create_delta_table(
                self.delta_table_name, self.IDCOLNAME, self.DMLCOLNAME,
                self._old_table.engine, self.old_column_list,
                self._old_table.name))
        self.add_drop_table_entry(self.delta_table_name)
        # We will break table into chunks when calculate checksums using
        # old primary key. We need this index to skip verify the same row
        # for multiple time if it has been changed a lot
        if self._pk_for_filter and not self.is_full_table_dump:
            self.execute_sql(
                sql.create_idx_on_delta_table(
                    self.delta_table_name, self._pk_for_filter))

    def create_insert_trigger(self):
        self.execute_sql(
            sql.create_insert_trigger(
                self.insert_trigger_name,
                self.table_name,
                self.delta_table_name,
                self.DMLCOLNAME, self.old_column_list,
                self.DML_TYPE_INSERT))
        self._cleanup_payload.add_drop_trigger_entry(
            self._current_db, self.insert_trigger_name)

    @wrap_hook
    def create_delete_trigger(self):
        self.execute_sql(
            sql.create_delete_trigger(
                self.delete_trigger_name, self.table_name,
                self.delta_table_name,
                self.DMLCOLNAME, self.old_column_list,
                self.DML_TYPE_DELETE))
        self._cleanup_payload.add_drop_trigger_entry(
            self._current_db, self.delete_trigger_name)

    def create_update_trigger(self):
        self.execute_sql(
            sql.create_update_trigger(
                self.update_trigger_name, self.table_name,
                self.delta_table_name,
                self.DMLCOLNAME, self.old_column_list,
                self.DML_TYPE_UPDATE, self.DML_TYPE_DELETE,
                self.DML_TYPE_INSERT, self._pk_for_filter))
        self._cleanup_payload.add_drop_trigger_entry(
            self._current_db, self.update_trigger_name)

    def check_long_trx(self):
        """
        Check if there's a long transaction running against table we'll touch.
        This is mainly for safety as long running transaction may block DDL,
        thus blocks more other requests
        """
        if self.skip_long_trx_check:
            return True
        processes = self.query(sql.show_processlist)
        for proc in processes:
            if not proc['Info']:
                sql_statement = ''
            else:
                sql_statement = proc['Info'].decode('utf-8', 'replace')
            if (proc.get('Time', 0) > self.long_trx_time and
                    proc.get('db', '') == self._current_db and
                    self.table_name in '--' + sql_statement and
                    not proc.get('Command', '') == 'Sleep'):
                raise OSCError('LONG_RUNNING_TRX',
                               {'pid': proc.get('Id', 0),
                                'user': proc.get('User', ''),
                                'host': proc.get('Host', ''),
                                'time': proc.get('Time', ''),
                                'command': proc.get('Command', ''),
                                'info': sql_statement})

    def is_repl_running(self):
        """
        Check current replication status. We need to know that exact state
        before we trying to stop the sql_thread. If the sql_thread is not
        stopped by us, then we'll skip starting it afterwards
        """
        result = self.query(sql.show_slave_status)
        if result:
            return all((result[0]['Slave_IO_Running'],
                       result[0]['Slave_SQL_Running']))
        else:
            return False

    def stop_slave_sql(self):
        """
        Stop sql_thread for such operations as create trigger and swap table
        """
        if self.is_repl_running():
            self.execute_sql(sql.stop_slave_sql)
            self.is_slave_stopped_by_me = True

    def start_slave_sql(self):
        """
        Start the sql_thread if we are the one stopped it
        """
        if self.is_slave_stopped_by_me:
            self.execute_sql(sql.start_slave_sql)
            self.is_slave_stopped_by_me = False

    def get_running_queries(self):
        """
        Get a list of running queries. A wrapper of a single query to make it
        easier for writing unittest
        """
        return self.query(sql.show_processlist)

    def kill_query_by_id(self, id):
        """
        Kill query with given query id. A wrapper of a single query to make it
        easier for writing unittest
        """
        self.execute_sql(sql.kill_proc, (id,))

    def kill_selects(self, table_name):
        """
        Kill current running SELECTs against the working database. So that
        they won't block the DDL statement we're about to execute
        """
        processlist = self.get_running_queries()
        for proc in processlist:
            if not proc['Info']:
                sql_statement = ''
            else:
                sql_statement = proc['Info'].decode('utf-8', 'replace')
            if (proc['db'] == self._current_db and
                    sql_statement and
                    table_name in sql_statement and
                    'information_schema' not in sql_statement.lower() and
                    ('select' in sql_statement.lower() or
                     'alter' in sql_statement.lower())):
                try:
                    self.kill_query_by_id(int(proc['Id']))
                except MySQLdb.MySQLError as e:
                    errcode, errmsg = e.args
                    # 1094: Unknown thread id
                    # This means the query we were trying to kill has finished
                    # before we run kill %d
                    if errcode == 1094:
                        log.info(
                            "Trying to kill query id: {}, but it has "
                            "already finished".format(proc['Id']))
                    else:
                        raise

    def start_transaction(self):
        """
        Start a transaction.
        """
        self.execute_sql(sql.start_transaction)

    def commit(self):
        """
        Commit and close the transaction
        """
        self.execute_sql(sql.commit)

    def ddl_guard(self):
        """
        If there're already too many concurrent queries running, it's probably
        a bad idea to run DDL. Wait for some time until they finished or
        we timed out
        """
        for _ in range(self.ddl_guard_attempts):
            result = self.query(sql.show_status, ('Threads_running',))
            if result:
                threads_running = int(result[0]['Value'])
                if threads_running > self.max_running_before_ddl:
                    log.warning(
                        "Threads running: {}, bigger than allowed: {}. "
                        "Sleep 1 second before check again."
                        .format(threads_running, self.max_running_before_ddl))
                    time.sleep(1)
                else:
                    log.debug(
                        "Threads running: {}, less than: {}. We are good "
                        "to go"
                        .format(threads_running, self.max_running_before_ddl))
                    return
        log.error(
            "Hit max attempts: {}, but the threads running still don't drop"
            "below: {}."
            .format(self.ddl_guard_attempts, self.max_running_before_ddl))
        raise OSCError('DDL_GUARD_FAILED')

    @wrap_hook
    def lock_tables(self, tables):
        for tablename in tables:
            self.kill_selects(tablename)
        for _ in range(self.lock_max_attempts):
            try:
                self.execute_sql(sql.lock_tables(tables))
                log.info("Successfully lock table(s) for write: {}"
                         .format(', '.join(tables)))
                break
            except MySQLdb.MySQLError as e:
                errcode, errmsg = e.args
                # 1205 is timeout and 1213 is deadlock
                if errcode in (1205, 1213):
                    log.warning(
                        "Retry locking because of error: {}".format(e))
                else:
                    raise
        else:
            # Cannot lock write after max lock attempts
            raise OSCError(
                'FAILED_TO_LOCK_TABLE',
                {'tables': ', '.join(tables)})

    def unlock_tables(self):
        self.execute_sql(sql.unlock_tables)
        log.info("Table(s) unlocked")

    @wrap_hook
    def create_triggers(self):
        self.stop_slave_sql()
        self.ddl_guard()
        log.debug('Locking table: {} before creating trigger'
                  .format(self.table_name))
        self.lock_tables(tables=[self.table_name])

        # Because we've already hold the WRITE LOCK on the table, it's now safe
        # to deal with operations that require metadata lock
        self.create_insert_trigger()
        self.create_delete_trigger()
        self.create_update_trigger()

        self.unlock_tables()
        self.start_slave_sql()

    @wrap_hook
    def start_snapshot(self):
        self.execute_sql(sql.start_transaction_with_snapshot)
        current_max = self.get_max_delta_id()
        log.info("Changes with id <= {} committed before dump snapshot, "
                 "and should be ignored.".format(current_max))
        # Only replay changes in this range (last_replayed_id, max_id_now]
        new_changes = self.query(
            sql.get_replay_row_ids(
                self.IDCOLNAME, self.DMLCOLNAME, self.delta_table_name),
            (self.last_replayed_id, current_max, ))
        self._replayed_chg_ids.extend([r[self.IDCOLNAME] for r in new_changes])
        self.last_replayed_id = current_max

    def affected_rows(self):
        return self._conn.conn.affected_rows()

    def refresh_range_start(self):
        self.execute_sql(
            sql.select_into(self.range_end_vars, self.range_start_vars))

    def select_full_table_into_outfile(self):
        stage_start_time = time.time()
        try:
            outfile = '{}.1'.format(self.outfile)
            affected_rows = self.execute_sql(
                sql.select_full_table_into_file(
                    self._pk_for_filter, self.table_name,
                    self.is_skip_fcache_supported, self.where),
                (outfile, ))
            self.outfile_suffix_end = 1
            self.stats['outfile_lines'] = affected_rows
            self._cleanup_payload.add_file_entry(outfile)
            self.commit()
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # 1086: File exists
            if errnum == 1086:
                raise OSCError('FILE_ALREADY_EXIST', {'file': outfile})
            else:
                raise
        self.stats['time_in_dump'] = time.time() - stage_start_time

    @wrap_hook
    def select_chunk_into_outfile(self, outfile, use_where):
        try:
            affected_rows = self.execute_sql(
                sql.select_full_table_into_file_by_chunk(
                    self.table_name,
                    self.range_start_vars_array,
                    self.range_end_vars_array,
                    self._pk_for_filter,
                    self.old_non_pk_column_list,
                    self.select_chunk_size,
                    use_where,
                    self.is_skip_fcache_supported,
                    self.where,
                    self._idx_name_for_filter
                ),
                (outfile, ))
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # 1086: File exists
            if errnum == 1086:
                raise OSCError('FILE_ALREADY_EXIST', {'file': outfile})
            else:
                raise
        log.debug("{} affected".format(affected_rows))
        self.stats['outfile_lines'] = affected_rows + \
            self.stats.setdefault('outfile_lines', 0)
        self.stats['outfile_cnt'] = 1 + \
            self.stats.setdefault('outfile_cnt', 0)
        self._cleanup_payload.add_file_entry(
            '{}.{}'.format(self.outfile, self.outfile_suffix_end))
        return affected_rows

    @wrap_hook
    def select_table_into_outfile(self):
        log.info("== Stage 2: Dump ==")
        stage_start_time = time.time()
        # We can not break the table into chunks when there's no existing pk
        # We'll have to use one big file for copy data
        if self.is_full_table_dump:
            return self.select_full_table_into_outfile()
        outfile_suffix = 1
        # To let the loop run at least once
        affected_rows = 1
        use_where = False
        printed_chunk = 0
        while affected_rows:
            self.outfile_suffix_end = outfile_suffix
            outfile = '{}.{}'.format(self.outfile, outfile_suffix)
            affected_rows = self.select_chunk_into_outfile(outfile, use_where)
            # Refresh where condition range for next select
            if affected_rows:
                self.refresh_range_start()
                use_where = True
                outfile_suffix += 1
            free_disk_space = util.spare_disk_size(self.outfile_dir)
            if free_disk_space < self.free_space_reserved:
                raise OSCError(
                    'NOT_ENOUGH_SPACE',
                    {'need': util.readable_size(self.free_space_reserved),
                     'avail': util.readable_size(free_disk_space)})
            progress_pct = int((float(outfile_suffix) / self.eta_chunks) * 100)
            progress_chunk = int(progress_pct / 10)
            if progress_chunk > printed_chunk and self.eta_chunks > 10:
                log.info("Dump progress: {}/{} chunks"
                         .format(outfile_suffix, self.eta_chunks))
                printed_chunk = progress_chunk
        self.commit()
        log.info("Dump finished")
        self.stats['time_in_dump'] = time.time() - stage_start_time

    @wrap_hook
    def drop_non_unique_indexes(self):
        """
        Drop non-unique indexes from the new table to speed up the load
        process
        """
        for idx in self.droppable_indexes:
            log.info("Dropping index '{}' on intermediate table"
                     .format(idx.name))
            self.ddl_guard()
            self.execute_sql(sql.drop_index(idx.name, self.new_table_name))

    @wrap_hook
    def load_chunk(self, column_list, chunk_id):
        self.execute_sql(
            sql.load_data_infile(
                self.new_table_name, column_list, ignore=self.eliminate_dups),
            ('{}.{}'.format(self.outfile, chunk_id),))
        # Delete the outfile once we have the data in new table to free
        # up space as soon as possible
        filepath = '{}.{}'.format(self.outfile, chunk_id)
        self.rm_file(filepath)
        self._cleanup_payload.remove_file_entry(filepath)

    def change_rocksdb_bulk_load(self, enable=True):
        # rocksdb_bulk_load relies on data being dumping in the same sequence
        # as new pk. If we are changing pk, then we cannot ensure that
        if self._old_table.primary_key != self._new_table.primary_key:
            log.warning("Skip rocksdb_bulk_load, because we are changing PK")
            return

        v = 1 if enable else 0
        try:
            self.execute_sql(
                sql.set_session_variable('rocksdb_bulk_load'), (v,))
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # 1193: unknown variable
            if errnum == 1193:
                log.warning(
                    "Failed to set rocksdb_bulk_load: {}".format(errmsg))
            else:
                raise

    @wrap_hook
    def load_data(self):
        stage_start_time = time.time()
        log.info("== Stage 3: Load data ==")
        # Generate the column name list string for load data infile
        # The column sequence is not exact the same as the original table.
        # It's pk_col_names + non_pk_col_name instead
        if self._pk_for_filter:
            if self.old_non_pk_column_list:
                column_list = self._pk_for_filter + self.old_non_pk_column_list
            else:
                column_list = self._pk_for_filter
        elif self.old_non_pk_column_list:
            column_list = self.old_non_pk_column_list
        else:
            # It's impossible to reach here, otherwise it means there's zero
            # column in old table which MySQL doesn't support. Something is
            # totally wrong if we get to this point
            raise OSCError('OSC_INTERNAL_ERROR',
                           {'msg': 'Unexpected scenario. Both _pk_for_filter '
                            'and old_non_pk_column_list are empty'})
        # Enable rocksdb bulk load before loading data
        if self._new_table.engine.upper() == 'ROCKSDB':
            self.change_rocksdb_bulk_load(enable=True)

        for suffix in range(1, self.outfile_suffix_end + 1):
            self.load_chunk(column_list, suffix)
            # Print out information after every 10% chunks have been loaded
            # We won't show progress if the number of chunks is less than 50
            if suffix % max(5, int(self.outfile_suffix_end / 10)) == 0:
                log.info("Load progress: {}/{} chunks"
                         .format(suffix, self.outfile_suffix_end))

        # disable rocksdb bulk load after loading data
        if self._new_table.engine.upper() == 'ROCKSDB':
            self.change_rocksdb_bulk_load(enable=False)
        self.stats['time_in_load'] = time.time() - stage_start_time

    def check_no_fcache_support(self):
        """
        Check whether current MySQL instance support SQL_NO_FCACHE
        which is only supported by WebScaleSQL
        """
        try:
            self.query(sql.select_sql_no_fcache(self.table_name))
            return True
        except Exception:
            # if any excpetion raised here, we'll treat it as
            # SQL_NO_FCACHE is not supported
            log.info("SQL_NO_FCACHE doesn't support in this MySQL")
            return False

    def check_max_statement_time_exists(self):
        """
        Check whether current MySQL instance support MAX_STATEMENT_TIME
        which is only supported by WebScaleSQL
        """
        # the max_statement_time is count in miliseconds
        try:
            self.query(sql.select_max_statement_time)
            return True
        except Exception:
            # if any excpetion raised here, we'll treat it as
            # MAX_STATEMENT_TIME is not supported
            log.info("MAX_STATEMENT_TIME doesn't support in this MySQL")
            return False

    def append_to_exclude_id(self):
        """
        Add all replayed IDs into tmp_table_exclude_id so that we won't replay
        again later
        """
        self.execute_sql(
            sql.insert_into_select_from(
                into_table=self.tmp_table_exclude_id,
                into_col_list=(self.IDCOLNAME, self.DMLCOLNAME),
                from_table=self.tmp_table_include_id,
                from_col_list=(self.IDCOLNAME, self.DMLCOLNAME)
            )
        )

    def get_max_delta_id(self):
        """
        Get current maximum delta table ID.
        """
        result = self.query(
            sql.get_max_id_from(self.IDCOLNAME, self.delta_table_name))
        # If no events has been replayed, max would return a string 'None'
        # instead of a pythonic None. So we should treat 'None' as 0 here
        if result[0]['max_id'] == 'None':
            return 0
        return result[0]['max_id']

    @wrap_hook
    def replay_delete_row(self, sql, *ids):
        """
        Replay delete type change

        @param sql:  SQL statement to replay the changes stored in chg table
        @type  sql:  string
        @param ids:  values of ID column from self.delta_table_name
        @type  ids:  list
        """
        affected_row = self.execute_sql(sql, ids)
        if not self.eliminate_dups and not self.where and \
                not self.skip_affected_rows_check:
            if not affected_row != 0:
                raise OSCError('REPLAY_WRONG_AFFECTED', {'num': affected_row})

    @wrap_hook
    def replay_insert_row(self, sql, *ids):
        """
        Replay insert type change

        @param sql:  SQL statement to replay the changes stored in chg table
        @type  sql:  string
        @param ids:  values of ID column from self.delta_table_name
        @type  ids:  list
        """
        affected_row = self.execute_sql(sql, ids)
        if not self.eliminate_dups and not self.where and \
                not self.skip_affected_rows_check:
            if not affected_row != 0:
                raise OSCError('REPLAY_WRONG_AFFECTED', {'num': affected_row})

    @wrap_hook
    def replay_update_row(self, sql, *ids):
        """
        Replay update type change

        @param sql:  SQL statement to replay the changes stored in chg table
        @type  sql:  string
        @param row:  single row of delta information from self.delta_table_name
        @type  row:  list
        """
        self.execute_sql(sql, ids)

    def get_gap_changes(self):
        # See if there're some gaps we need to cover. Because there're some
        # transactions that may started before last replay snapshot but
        # committed afterwards, which will cause __OSC_ID_ smaller than
        # self.last_replayed_id
        delta = []
        log.info(
            "Checking {} gap ids".format(
                len(self._replayed_chg_ids.missing_points())))
        for chg_id in self._replayed_chg_ids.missing_points():
            row = self.query(
                sql.get_chg_row(
                    self.IDCOLNAME, self.DMLCOLNAME, self.delta_table_name),
                (chg_id,))
            if bool(row):
                log.debug("Change {} appears now!".format(chg_id))
                delta.append(row[0])
        for row in delta:
            self._replayed_chg_ids.fill(row[self.IDCOLNAME])
        log.info(
            "{} changes before last checkpoint ready for replay"
            .format(len(delta)))
        return delta

    def divide_changes_to_group(self, chg_rows):
        """
        Put consecutive changes with the same type into a group so that we can
        execute them in a single query to speed up replay

        @param chg_rows:  list of rows returned from _chg select query
        @type  chg_rows:  list[dict]
        """
        id_group = []
        type_now = None
        for idx, chg in enumerate(chg_rows):
            # Start of the current group
            if type_now is None:
                type_now = chg[self.DMLCOLNAME]
            id_group.append(chg[self.IDCOLNAME])

            # Dump when we are at the end of the changes
            if idx == len(chg_rows) - 1:
                yield type_now, id_group
                return
            # update type cannot be grouped
            elif type_now == self.DML_TYPE_UPDATE:
                yield type_now, id_group
                type_now = None
                id_group = []
            # The next change is a different type, dump what we have now
            elif chg_rows[idx + 1][self.DMLCOLNAME] != type_now:
                yield type_now, id_group
                type_now = None
                id_group = []
            # Reach the max group size, let's submit the query for now
            elif len(id_group) >= self.replay_group_size:
                yield type_now, id_group
                type_now = None
                id_group = []
            # The next element will be the same as what we are now
            else:
                continue

    def replay_changes(self, single_trx=False, holding_locks=False):
        """
        Loop through all the existing events in __osc_chg table and replay
        the change

        @param single_trx:  Replay all the changes in single transaction or
        not
        @type  single_trx:  bool
        """
        stage_start_time = time.time()
        log.debug("Timeout for replay changes: {}".format(self.replay_timeout))
        time_start = time.time()
        deleted, inserted, updated = 0, 0, 0

        # all the changes to be replayed in this round will be stored in
        # tmp_table_include_id. Though change events may keep being generated,
        # we'll only replay till the end of temporary table
        if single_trx and not self.bypass_replay_timeout and \
                self.check_max_statement_time_exists():
            replay_ms = self.replay_timeout * 1000
        else:
            replay_ms = None
        max_id_now = self.get_max_delta_id()
        if self.detailed_mismatch_info or self.dump_after_checksum:
            # We need this information for better understanding of the checksum
            # mismatch issue
            log.info("Replaying changes happened before change ID: {}"
                     .format(max_id_now))
        delta = self.get_gap_changes()

        # Only replay changes in this range (last_replayed_id, max_id_now]
        new_changes = self.query(
            sql.get_replay_row_ids(
                self.IDCOLNAME, self.DMLCOLNAME, self.delta_table_name,
                replay_ms
            ),
            (self.last_replayed_id, max_id_now, ))
        self._replayed_chg_ids.extend([r[self.IDCOLNAME] for r in new_changes])
        delta.extend(new_changes)

        log.info("Total {} changes to replay".format(len(delta)))
        # Generate all three possible replay SQL here, so that we don't waste
        # CPU time regenerating them for each replay event
        delete_sql = sql.replay_delete_row(
            self.new_table_name, self.delta_table_name, self.IDCOLNAME,
            self._pk_for_filter
        )
        update_sql = sql.replay_update_row(
            self.old_non_pk_column_list, self.new_table_name,
            self.delta_table_name, self.eliminate_dups,
            self.IDCOLNAME, self._pk_for_filter
        )
        insert_sql = sql.replay_insert_row(
            self.old_column_list, self.new_table_name,
            self.delta_table_name, self.IDCOLNAME,
            self.eliminate_dups
        )
        replayed = 0
        replayed_total = 0
        showed_pct = 0
        for chg_type, ids in self.divide_changes_to_group(delta):
            # We only care about replay time when we are holding a write lock
            if holding_locks and not self.bypass_replay_timeout and \
                    time.time() - time_start > self.replay_timeout:
                raise OSCError("REPLAY_TIMEOUT")
            replayed_total += len(ids)
            # Commit transaction after every replay_batch_szie number of
            # changes have been replayed
            if not single_trx and replayed > self.replay_batch_size:
                self.commit()
                self.start_transaction()
            else:
                replayed += len(ids)

            # Use corresponding SQL to replay each type of changes
            if chg_type == self.DML_TYPE_DELETE:
                self.replay_delete_row(delete_sql, ids)
                deleted += len(ids)
            elif chg_type == self.DML_TYPE_UPDATE:
                self.replay_update_row(update_sql, ids)
                updated += len(ids)
            elif chg_type == self.DML_TYPE_INSERT:
                self.replay_insert_row(insert_sql, ids)
                inserted += len(ids)
            else:
                # We are not supposed to reach here, unless someone explicitly
                # insert a row with unknown type into _chg table during OSC
                raise OSCError("UNKOWN_REPLAY_TYPE",
                               {'type_value': chg_type})
            # Print progress information after every 10% changes have been
            # replayed. If there're no more than 100 changes to replay then
            # there'll be no such progress information
            progress_pct = int(replayed_total / len(delta) * 100)
            if progress_pct > showed_pct:
                log.info(
                    "Load progress: {}/{} changes"
                    .format(replayed_total + 1, len(delta)))
                showed_pct += 10
        # Commit for last batch
        if not single_trx:
            self.commit()
        self.last_replayed_id = max_id_now

        time_spent = time.time() - stage_start_time
        self.stats['time_in_replay'] = \
            self.stats.setdefault('time_in_replay', 0) + time_spent
        log.info("Replayed {} INSERT, {} DELETE, {} UPDATE in {:.2f} Seconds"
                 .format(inserted, deleted, updated, time_spent))

    def set_innodb_tmpdir(self, innodb_tmpdir):
        try:
            self.execute_sql(
                sql.set_session_variable('innodb_tmpdir'), (innodb_tmpdir,))
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # data_dir cannot always be set to innodb_tmpdir due to
            # priviledge issue. Falling back to tmpdir if it happens
            # 1193: unknown variable
            # 1231: Failed to set because of priviledge error
            if errnum in (1231, 1193):
                log.warning(
                    "Failed to set innodb_tmpdir, falling back to tmpdir: {}"
                    .format(errmsg))
            else:
                raise

    @wrap_hook
    def recreate_non_unique_indexes(self):
        """
        Re-create non-unique indexes onto the new table
        """
        # Skip replaying changes for now, if don't have to recreate index
        if not self.droppable_indexes:
            return

        log.info(
            "Replay changes before recreating indexes as they can be "
            "executed faster before that")
        self.replay_changes(single_trx=False)
        self.set_innodb_tmpdir(self.outfile_dir)
        # Execute alter table only if we have index to create
        if self.droppable_indexes:
            self.ddl_guard()
            log.info(
                "Recreating indexes: {}".format(', '.join(
                    col.name for col in self.droppable_indexes)))
            self.execute_sql(
                sql.add_index(self.new_table_name, self.droppable_indexes))

    @wrap_hook
    def analyze_table(self):
        """
        Force to update internal optimizer statistics. So that we are less
        likely to hit bad execution plan because too many changes have been
        made
        """

        # Analyze table has a query result, we have to use query here.
        # Otherwise we'll get a out of sync error
        self.query(sql.analyze_table(self.new_table_name))

    def compare_checksum(self, old_table_checksum, new_table_checksum):
        """
        Given two list of checksum result generated by checksum_by_chunk,
        compare whether there's any difference between them

        @param old_table_checksum:  checksum from old table
        @type  old_table_checksum:  list of list

        @param new_table_checksum:  checksum from new table
        @type  new_table_checksum:  list of list

        """
        if len(old_table_checksum) != len(new_table_checksum):
            log.error("The total number of checksum chunks mismatch "
                      "OLD={}, NEW={}"
                      .format(len(old_table_checksum),
                              len(new_table_checksum)))
            raise OSCError('CHECKSUM_MISMATCH')
        log.info("{} checksum chunks in total"
                 .format(len(old_table_checksum)))

        for idx, checksum_entry in enumerate(old_table_checksum):
            for col in checksum_entry:
                if not old_table_checksum[idx][col] == \
                        new_table_checksum[idx][col]:
                    log.error(
                        "checksum/count mismatch for chunk {} "
                        "column `{}`: OLD={}, NEW={}"
                        .format(
                            idx, col,
                            old_table_checksum[idx][col],
                            new_table_checksum[idx][col]))
                    log.error(
                        "Number of rows for the chunk that cause the "
                        "mismatch: OLD={}, NEW={}"
                        .format(old_table_checksum[idx]['cnt'],
                                new_table_checksum[idx]['cnt']))
                    log.error(
                        "Current replayed max(__OSC_ID) of chg table {}"
                        .format(self.last_replayed_id)
                    )
                    raise OSCError('CHECKSUM_MISMATCH')

    def checksum_full_table(self):
        """
        Running checksum in single query, this will be used only for tables
        which don't have primary in the old schema. See checksum_by_chunk
        for more detail
        """
        # Calculate checksum for old table
        old_checksum = self.query(
            sql.checksum_full_table(
                self.table_name, self._old_table.column_list))

        # Calculate checksum for new table
        new_checksum = self.query(
            sql.checksum_full_table(
                self.new_table_name, self._old_table.column_list))
        self.commit()

        # Compare checksum
        if old_checksum and new_checksum:
            self.compare_checksum(old_checksum, new_checksum)

    def checksum_for_single_chunk(
            self, table_name, use_where, idx_for_checksum):
        """
        Using the same set of session variable as chunk start point and
        calculate checksum for old table/new table. If assign is provided,
        current right boundry will be passed into range_start_vars as the
        start of next chunk
        """
        return self.query(
            sql.checksum_by_chunk_with_assign(
                table_name, self.checksum_column_list,
                self._pk_for_filter,
                self.range_start_vars_array, self.range_end_vars_array,
                self.select_chunk_size, use_where,
                idx_for_checksum))[0]

    def dump_current_chunk(self, use_where):
        """
        Use select into outfile to dump the data in the previous chunk that
        caused checksum mismatch

        @param use_where: whether we should use session variable as selection
        boundary in where condition
        @type use_where: bool
        """
        log.info("Dumping raw data onto local disk for further investigation")
        log.info("Columns will be dumped in following order: ")
        log.info(", ".join(self._pk_for_filter + self.checksum_column_list))
        for table_name in [self.table_name, self.new_table_name]:
            if table_name == self.new_table_name:
                # index for new scehma can be any indexes that provides
                # uniqueness and covering old PK lookup
                idx_for_checksum = self.find_coverage_index()
                outfile = '{}.new'.format(self.outfile)
            else:
                # index for old schema should always be PK
                idx_for_checksum = 'PRIMARY'
                outfile = '{}.old'.format(self.outfile)
            log.info("Dump offending chunk from {} into {}"
                     .format(table_name, outfile))
            self.execute_sql(
                sql.dump_current_chunk(
                    table_name, self.checksum_column_list,
                    self._pk_for_filter,
                    self.range_start_vars_array,
                    self.select_chunk_size,
                    idx_for_checksum, use_where), (outfile,))

    @wrap_hook
    def detailed_checksum(self):
        """
        Yet another way of calculating checksum but it opens a longer trx
        than the default approach. By doing this we will able to print out
        the exact chunk of data that caused a checksum mismatch
        """
        affected_rows = 1
        use_where = False
        new_idx_for_checksum = self.find_coverage_index()
        old_idx_for_checksum = 'PRIMARY'
        chunk_id = 0
        while(affected_rows):
            chunk_id += 1
            old_checksum = self.checksum_for_single_chunk(
                self.table_name, use_where, old_idx_for_checksum)
            new_checksum = self.checksum_for_single_chunk(
                self.new_table_name, use_where, new_idx_for_checksum)
            affected_rows = old_checksum['_osc_chunk_cnt']
            if old_checksum.values() != new_checksum.values():
                log.info(
                    "Checksum mismatch detected for chunk {}: "
                    .format(chunk_id))
                log.info("OLD: {}".format(str(old_checksum)))
                log.info("NEW: {}".format(str(new_checksum)))
                self.dump_current_chunk(use_where)
                raise OSCError('CHECKSUM_MISMATCH')

            # Refresh where condition range for next select
            if affected_rows:
                self.refresh_range_start()
                use_where = True

    @wrap_hook
    def checksum_by_chunk(self, table_name, dump_after_checksum=False):
        """
        Running checksum for all the existing data in new table. This is to
        make sure there's no data corruption after load and first round of
        replay
        """
        checksum_result = []
        # Checksum by chunk. This is pretty much the same logic as we've used
        # in select_table_into_outfile
        affected_rows = 1
        use_where = False
        outfile_id = 0
        if table_name == self.new_table_name:
            idx_for_checksum = self.find_coverage_index()
            outfile_prefix = "{}.new".format(self.outfile)
        else:
            idx_for_checksum = self._idx_name_for_filter
            outfile_prefix = "{}.old".format(self.outfile)
        while(affected_rows):
            checksum = self.query(
                sql.checksum_by_chunk(
                    table_name, self.checksum_column_list,
                    self._pk_for_filter,
                    self.range_start_vars_array, self.range_end_vars_array,
                    self.select_chunk_size, use_where,
                    self.is_skip_fcache_supported,
                    idx_for_checksum))
            # Dump the data onto local disk for further investigation
            # This will be very helpful when there's a reproducable checksum
            # mismatch issue
            if dump_after_checksum:
                self.execute_sql(
                    sql.dump_current_chunk(
                        table_name, self.checksum_column_list,
                        self._pk_for_filter,
                        self.range_start_vars_array,
                        self.select_chunk_size,
                        idx_for_checksum, use_where),
                    ('{}.{}'.format(outfile_prefix, str(outfile_id)), )
                )
                outfile_id += 1

            # Refresh where condition range for next select
            if checksum:
                self.refresh_range_start()
                affected_rows = checksum[0]['cnt']
                checksum_result.append(checksum[0])
                use_where = True
        return checksum_result

    def need_checksum(self):
        """
        Check whether we should checksum or not
        """
        if self.skip_checksum:
            log.warning("Skip checksum because --skip-checksum is specified")
            return False
        # There's no point running a checksum compare for selective dump
        if self.where:
            log.warning("Skip checksum because --where is given")
            return False
        # If the collation of primary key column has been changed, then
        # it's high possible that the checksum will mis-match, because
        # the returning sequence after order by primary key may be vary
        # for different collations
        for pri_column in self._pk_for_filter:
            old_column_tmp = [col for col in self._old_table.column_list
                              if col.name == pri_column]
            if old_column_tmp:
                old_column = old_column_tmp[0]
            new_column_tmp = [col for col in self._new_table.column_list
                              if col.name == pri_column]
            if new_column_tmp:
                new_column = new_column_tmp[0]
            if old_column and new_column:
                if not is_equal(old_column.collate, new_column.collate):
                    log.warning(
                        "Collation of primary key column {} has been "
                        "changed. Skip checksum ".format(old_column.name))
                    return False
        # There's no way we can run checksum by chunk if the primary key cannot
        # be covered by any index of the new schema
        if not self.validate_post_alter_pk():
            if self.skip_pk_coverage_check:
                log.warning(
                    "Skipping checksuming because there's no unique index "
                    "in new table schema can perfectly cover old primary key "
                    "combination for search"
                    .format(old_column.name))
                return False
        else:
            # Though we have enough coverage for primary key doesn't
            # necessarily mean we can use it for checksum, it has to be an
            # unique index as well. Skip checksum if there's no such index
            if not self.find_coverage_index():
                log.warning(
                    "Skipping checksuming because there's no unique index "
                    "in new table schema can perfectly cover old primary key "
                    "combination for search"
                    .format(old_column.name))
                return False
        return True

    def need_checksum_for_changes(self):
        """
        Check whether we should checksum for changes or not
        """
        # We don't need to run checksum for changes, if we don't want checksum
        # at all
        if not self.need_checksum():
            return False
        if self.is_full_table_dump:
            log.warning(
                "We're adding new primary key to the table. Skip running "
                "checksum for changes, because that's inefficient")
            return False
        return True

    @wrap_hook
    def checksum(self):
        """
        Run checksum for all existing data in new table.
        We will do another around of checksum, but only for changes happened
        in between
        """
        log.info("== Stage 4: Checksum ==")
        if not self.need_checksum():
            return

        stage_start_time = time.time()
        if self.eliminate_dups:
            log.warning("Skip checksum, because --eliminate-duplicate "
                        "specified")
            return

        # Replay outside of transaction so that we won't hit max allowed
        # transaction time,
        log.info("= Stage 4.1: Catch up before generating checksum =")
        self.replay_till_good2go(checksum=False)

        log.info("= Stage 4.2: Comparing checksum =")
        self.start_transaction()
        # To fill the gap between old and new table since last replay
        log.info("Replay changes to bring two tables to a comparable state")
        self.replay_changes(single_trx=True)

        # if we don't have a PK on old schema, then we are not able to checksum
        # by chunk. We'll do a full table scan for checksum instead
        if self.is_full_table_dump:
            return self.checksum_full_table()

        if not self.detailed_mismatch_info:
            log.info("Checksuming data from old table")
            old_table_checksum = self.checksum_by_chunk(
                self.table_name,
                dump_after_checksum=self.dump_after_checksum)

            # We can calculate the checksum for new table outside the
            # transaction, because the data in new table is static without
            # replaying chagnes
            self.commit()

            log.info("Checksuming data from new table")
            new_table_checksum = self.checksum_by_chunk(
                self.new_table_name,
                dump_after_checksum=self.dump_after_checksum)

            log.info("Compare checksum")
            self.compare_checksum(old_table_checksum, new_table_checksum)
        else:
            self.detailed_checksum()

        self.last_checksumed_id = self.last_replayed_id

        log.info("Checksum match between new and old table")
        self.stats['time_in_table_checksum'] = time.time() - stage_start_time

    @wrap_hook
    def replay_till_good2go(self, checksum):
        """
        Keep replaying changes until the time spent in replay is below
        self.replay_timeout
        For table which has huge numbers of writes during OSC, we'll probably
        hit replay timeout if we call swap_tables directly after checksum.
        We will do several round iteration here in order to bring the number
        of un-played changes down to a proper level a proper level

        @param checksum:  Run checksum for replayed changes or not
        @type  checksum:  bool

        """
        log.info("Replay at most {} more round(s) until we can finish in {} "
                 "seconds"
                 .format(self.replay_max_attempt, self.replay_timeout))
        for i in range(self.replay_max_attempt):
            log.info("Catchup Attempt: {}".format(i + 1))
            start_time = time.time()
            self.start_transaction()
            self.replay_changes(single_trx=False)
            # If checksum is required, then we need to make sure total time
            # spent in replay+checksum is below replay_timeout.
            if checksum:
                self.start_transaction()
                log.info("Catch up in order to compare checksum for the "
                         "rows that have been changed")
                self.replay_changes(single_trx=True)
                self.checksum_for_changes(single_trx=False)
            time_in_replay = time.time() - start_time
            if time_in_replay < self.replay_timeout:
                log.info("Time spent in last round of replay is {:.2f}, which "
                         "is less than replay_timeout: {} for final replay. "
                         "We are good to proceed"
                         .format(time_in_replay, self.replay_timeout))
                break
        else:
            # We are not able to bring the replay time down to replay_timeout
            if not self.bypass_replay_timeout:
                raise OSCError('MAX_ATTEMPT_EXCEEDED',
                               {'timeout': self.replay_timeout})
            else:
                log.warning("Proceed after max replay attempts exceeded. "
                            "Because --bypass-replay-timeout is specified")

    @wrap_hook
    def checksum_by_replay_chunk(self, table_name):
        """
        Run checksum for rows which have been touched by changes made after
        last round of checksum.
        """
        # Generate a column string which contains all non-changed columns
        # wrapped with checksum function.
        checksum_result = []
        id_limit = self.last_checksumed_id
        # Using the same batch size for checksum as we used for replaying
        while id_limit < self.last_replayed_id:
            result = self.query(
                sql.checksum_by_replay_chunk(
                    table_name, self.delta_table_name,
                    self.old_column_list, self._pk_for_filter,
                    self.IDCOLNAME, id_limit, self.last_replayed_id,
                    self.replay_batch_size
                ))
            checksum_result.append(result[0])
            id_limit += self.replay_batch_size
        return checksum_result

    @wrap_hook
    def checksum_for_changes(self, single_trx=False):
        """
        This checksum will only run against changes made between last full
        table checksum and before swap table
        We assume A transaction has been opened before calling this function,
        and changes has been replayed

        @param single_trx:  whether skip the commit call after checksum old
        table. This can prevent opening a transaction for too long when we
        don't actually need it
        @type  single_trx:  bool

        """
        if self.eliminate_dups:
            log.warning("Skip checksum, because --elimiate-duplicate "
                        "specified")
            return
        elif not self.need_checksum_for_changes():
            return
        # Because chunk checksum use old pk combination for searching row
        # If we don't have a pk/uk on old table then it'll be very slow, so we
        # have to skip here
        elif self.is_full_table_dump:
            return
        else:
            log.info("Running checksum for rows have been changed since "
                     "last checksum from change ID: {}"
                     .format(self.last_checksumed_id))
        start_time = time.time()
        old_table_checksum = self.checksum_by_replay_chunk(self.table_name)
        # Checksum for the __new table should be issued inside the transcation
        # too. Otherwise those invisible gaps in the __chg table will show
        # up when calculating checksums
        new_table_checksum = self.checksum_by_replay_chunk(self.new_table_name)
        # After calculation checksums from both tables, we now can close the
        # transcation, if we want
        if not single_trx:
            self.commit()
        self.compare_checksum(old_table_checksum, new_table_checksum)
        self.last_checksumed_id = self.last_replayed_id
        self.stats['time_in_delta_checksum'] = \
            self.stats.setdefault('time_in_delta_checksum', 0) + \
            (time.time() - start_time)

    @wrap_hook
    def swap_tables(self):
        """
        Flip the table name while holding the write lock. All operations
        during this stage will be executed inside a single transaction.
        """
        if self.stop_before_swap:
            return True
        log.info("== Stage 6: Swap table ==")
        self.stop_slave_sql()
        self.execute_sql(sql.set_session_variable('autocommit'), (0,))
        self.start_transaction()
        stage_start_time = time.time()
        self.lock_tables((self.new_table_name, self.table_name,
                          self.delta_table_name))
        log.info("Final round of replay before swap table")
        self.replay_changes(single_trx=True, holding_locks=True)
        # We will not run delta checksum here, because there will be an error
        # like this, if we run a nested query using `NOT EXISTS`:
        # SQL execution error: [1100] Table 't' was not locked with LOCK TABLES
        self.execute_sql(
            sql.rename_table(self.table_name, self.renamed_table_name))
        self.add_drop_table_entry(self.renamed_table_name)
        self.execute_sql(
            sql.rename_table(self.new_table_name, self.table_name))
        log.info("Table has successfully swapped, new schema takes effect now")
        self._cleanup_payload.remove_drop_table_entry(
            self._current_db, self.new_table_name)
        self.commit()
        self.unlock_tables()
        self.stats['time_in_lock'] = \
            self.stats.setdefault('time_in_lock', 0) + \
            (time.time() - stage_start_time)
        self.execute_sql(sql.set_session_variable('autocommit'), (1,))
        self.start_slave_sql()

    @wrap_hook
    def cleanup(self):
        """
        Cleanup all the temporary thing we've created so far
        """
        log.info("== Stage 7: Cleanup ==")
        # Close current connection to free up all the temporary resource
        # and locks
        try:
            self.start_slave_sql()
            self.release_osc_lock()
            self.close_conn()
        except Exception:
            log.exception(
                "Ignore following exception, because we want to try our "
                "best to cleanup, and free disk space:")
        self._cleanup_payload.mysql_user = self.mysql_user
        self._cleanup_payload.mysql_pass = self.mysql_pass
        self._cleanup_payload.socket = self.socket
        self._cleanup_payload.get_conn_func = self.get_conn_func
        self._cleanup_payload.cleanup(self._current_db)

    def print_stats(self):
        log.info("Time in dump: {:.3f}s"
                 .format(self.stats.get('time_in_dump', 0)))
        log.info("Time in load: {:.3f}s"
                 .format(self.stats.get('time_in_load', 0)))
        log.info("Time in replay: {:.3f}s"
                 .format(self.stats.get('time_in_replay', 0)))
        log.info("Time in table checksum: {:.3f}s"
                 .format(self.stats.get('time_in_table_checksum', 0)))
        log.info("Time in delta checksum: {:.3f}s"
                 .format(self.stats.get('time_in_delta_checksum', 0)))
        log.info("Time holding locks: {:.3f}s"
                 .format(self.stats.get('time_in_lock', 0)))

    @wrap_hook
    def run_ddl(self, db, sql):
        try:
            time_started = time.time()
            self._new_table = parse_create(sql)
            self._current_db = db
            self.init_connection(db)
            self.init_table_obj()
            self.determine_outfile_dir()
            if self.force_cleanup:
                self.cleanup_with_force()
            if self.has_desired_schema():
                return
            self.pre_osc_check()
            self.check_long_trx()
            self.create_copy_table()
            self.create_delta_table()
            self.create_triggers()
            self.start_snapshot()
            self.select_table_into_outfile()
            self.drop_non_unique_indexes()
            self.load_data()
            self.recreate_non_unique_indexes()
            self.analyze_table()
            self.checksum()
            log.info("== Stage 5: Catch up to reduce time for holding lock ==")
            self.replay_till_good2go(checksum=True)
            self.swap_tables()
            self.cleanup()
            self.print_stats()
            self.stats['wall_time'] = time.time() - time_started
        except (MySQLdb.OperationalError, MySQLdb.ProgrammingError,
                MySQLdb.IntegrityError) as e:
            errnum, errmsg = e.args
            log.error(
                "SQL execution error: [{}] {}\n"
                "When executing: {}\n"
                "With args: {}"
                .format(errnum, errmsg, self._sql_now, self._sql_args_now))
            # 2013 stands for lost connection to MySQL
            # 2006 stands for MySQL has gone away
            # Both means we have been killed
            if errnum in (2006, 2013) and self.skip_cleanup_after_kill:
                # Only skip dropping table, leave trigger around my break
                # replication which is really bad
                self._cleanup_payload.remove_drop_table_entry(
                    self._current_db, self.new_table_name)
                self._cleanup_payload.remove_drop_table_entry(
                    self._current_db, self.delta_table_name)
            if not self.keep_tmp_table:
                self.cleanup()
            raise OSCError('GENERIC_MYSQL_ERROR',
                           {'stage': "running DDL on db '{}'".format(db),
                            'errnum': errnum,
                            'errmsg': errmsg},
                           mysql_err_code=errnum)
        except Exception as e:
            log.exception(
                "{0} Exception raised, start to cleanup before exit {0}"
                .format("-" * 10))
            # We want keep the temporary table for further investigation
            if not self.keep_tmp_table:
                self.cleanup()
            if not isinstance(e, OSCError):
                # It's a python exception
                raise OSCError('OSC_INTERNAL_ERROR', {'msg': str(e)})
            else:
                raise
