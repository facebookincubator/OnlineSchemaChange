#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import gc
import glob
import logging
import os
import re
import time
from copy import deepcopy
from threading import Timer
from typing import collections, List, Optional, Set

import MySQLdb
from libfb.py.decorators import retryable
from MySQLdb.constants import ER as mysql_errors
from osc.lib.payload.osc_catchup_tool import OscCatchupTool
from osc.lib.sqlparse.diff import IndexAlterType

from .. import constant, sql, util
from ..error import OSCError
from ..hook import wrap_hook
from ..sqlparse import is_equal, need_default_ts_bootstrap, ParseError, SchemaDiff
from .base import Payload
from .cleanup import CleanupPayload

log: logging.Logger = logging.getLogger(__name__)
BulkLoadParams = collections.namedtuple(
    "BulkLoadParams",
    [
        "should_disable_bulk_load",
        "use_bulk_load_with_pk_charset",
        "use_bulk_load_with_uk_check",
    ],
)


class CopyPayload(Payload):
    """
    This payload implements the actual OSC logic. Basically it'll create a new
    physical table and then load data into it while it keeps the original table
    serving read/write requests. Later it will replay the changes captured
    by trigger onto the new table. Finally, a table name flip will be
    issued to make the new schema serve requests

    Properties in this class have consistent name convention. A property name
    will look like:
        [old/new]_[pk/non_pk]_column_list
    with:
        - old/new representing which schema these columns are from, old or new
        - pk/non_pk representing whether these columns are a part of primary
            key
    """

    IDCOLNAME = "_osc_ID_"
    DMLCOLNAME = "_osc_dml_type_"

    DML_TYPE_INSERT = 1
    DML_TYPE_DELETE = 2
    DML_TYPE_UPDATE = 3

    def __init__(self, *args, **kwargs):
        super(CopyPayload, self).__init__(*args, **kwargs)
        self._pk_for_filter = []
        self._idx_name_for_filter = "PRIMARY"
        self._new_table = None
        self._old_table = None
        self._replayed_chg_ids = util.RangeChain()
        self.select_chunk_size = 0
        self.use_batch_updates = False
        self.select_checksum_chunk_size = 0
        self.bypass_replay_timeout = False
        self.is_ttl_disabled_by_me = False
        self.stop_before_swap = False
        self.outfile_suffix_end = 0
        self.outfile_suffix_start = 0
        self.last_replayed_id = 0
        self.current_gtid_set = ""
        self.last_checksumed_id = 0
        self.current_checksum_record = -1
        self.table_size = 0
        self.session_overrides = []
        self.disable_replication = kwargs.get("disable_replication", True)
        self._cleanup_payload = CleanupPayload(*args, **kwargs)
        self.stats = {}
        self.partitions = {}
        self.eta_chunks = 1
        self._last_kill_timer = None
        self.table_swapped = False
        self.current_catchup_start_time = 0
        self.current_catchup_end_time = 0
        self.max_id_to_replay_upto_for_good2go = -1
        self.under_transaction = False
        self.checksum_required_for_replay = False

        self.repl_status: str = kwargs.get("repl_status", "")
        self.outfile_dir: str = kwargs.get("outfile_dir", "")
        # By specify this option we are allowed to open a long transaction
        # during full table dump and full table checksum
        self.allow_new_pk: bool = kwargs.get("allow_new_pk", False)
        self.allow_drop_column: bool = kwargs.get("allow_drop_column", False)
        self.detailed_mismatch_info: bool = kwargs.get("detailed_mismatch_info", False)
        self.dump_after_checksum: bool = kwargs.get("dump_after_checksum", False)
        # Whether to ignore unique key violations and silently drop such rows.
        # This implies checksum will be skipped, since it cannot match.
        self.eliminate_dups: bool = kwargs.get("eliminate_dups", False)
        self.rm_partition: bool = kwargs.get("rm_partition", False)
        self.force_cleanup: bool = kwargs.get("force_cleanup", False)
        self.skip_cleanup_after_kill: bool = kwargs.get(
            "skip_cleanup_after_kill", False
        )

        # TODO: are these used?
        self.pre_load_statement: str = kwargs.get("pre_load_statement", "")
        self.post_load_statement: str = kwargs.get("post_load_statement", "")

        self.replay_max_attempt: int = kwargs.get(
            "replay_max_attempt", constant.DEFAULT_REPLAY_ATTEMPT
        )
        self.replay_timeout: int = kwargs.get(
            "replay_timeout", constant.REPLAY_DEFAULT_TIMEOUT
        )
        self.replay_batch_size: int = kwargs.get(
            "replay_batch_size", constant.DEFAULT_BATCH_SIZE
        )
        self.replay_group_size: int = kwargs.get(
            "replay_group_size", constant.DEFAULT_REPLAY_GROUP_SIZE
        )
        self.skip_pk_coverage_check: bool = kwargs.get("skip_pk_coverage_check", False)
        self.pk_coverage_size_threshold: int = kwargs.get(
            "pk_coverage_size_threshold", constant.PK_COVERAGE_SIZE_THRESHOLD
        )
        self.skip_long_trx_check: bool = kwargs.get("skip_long_trx_check", False)

        # TODO: Is this actually needed here?
        self.ddl_file_list: str = kwargs.get("ddl_file_list", "")

        self.free_space_reserved_percent: int = kwargs.get(
            "free_space_reserved_percent", constant.DEFAULT_RESERVED_SPACE_PERCENT
        )
        self.long_trx_time: int = kwargs.get("long_trx_time", constant.LONG_TRX_TIME)
        self.max_running_before_ddl: int = kwargs.get(
            "max_running_before_ddl", constant.MAX_RUNNING_BEFORE_DDL
        )
        self.ddl_guard_attempts: int = kwargs.get(
            "ddl_guard_attempts", constant.DDL_GUARD_ATTEMPTS
        )
        self.lock_max_attempts: int = kwargs.get(
            "lock_max_attempts", constant.LOCK_MAX_ATTEMPTS
        )
        self.lock_max_wait_before_kill_seconds: float = kwargs.get(
            "lock_max_wait_before_kill_seconds",
            constant.LOCK_MAX_WAIT_BEFORE_KILL_SECONDS,
        )
        self.session_timeout: int = kwargs.get(
            "mysql_session_timeout", constant.SESSION_TIMEOUT
        )
        self.idx_recreation: bool = kwargs.get("idx_recreation", False)
        self.rocksdb_bulk_load_allow_sk: bool = kwargs.get(
            "rocksdb_bulk_load_allow_sk", False
        )
        self.unblock_table_creation_without_pk: bool = kwargs.get(
            "unblock_table_creation_without_pk", False
        )
        self.rebuild: bool = kwargs.get("rebuild", False)
        self.keep_tmp_table: bool = kwargs.get("keep_tmp_table_after_exception", False)
        self.skip_checksum: bool = kwargs.get("skip_checksum", False)
        self.skip_checksum_for_modified: bool = kwargs.get(
            "skip_checksum_for_modified", False
        )
        self.skip_delta_checksum: bool = kwargs.get("skip_delta_checksum", False)

        # Whether to use the server-native CHECKSUM TABLE statement.
        self.use_checksum_statement: bool = kwargs.get("use_checksum_statement", False)
        self.skip_named_lock: bool = kwargs.get("skip_named_lock", False)
        self.skip_affected_rows_check: bool = kwargs.get(
            "skip_affected_rows_check", False
        )

        # Debugging only
        self.skip_chunk_cleanup = False
        self.where: str | None = kwargs.get("where", None)
        self.session_overrides_str: str = kwargs.get("session_overrides", "")
        self.fail_for_implicit_conv: bool = kwargs.get("fail_for_implicit_conv", False)
        self.max_wait_for_slow_query: int = kwargs.get(
            "max_wait_for_slow_query", constant.MAX_WAIT_FOR_SLOW_QUERY
        )
        self.max_replay_batch_size: int = kwargs.get(
            "max_replay_batch_size", constant.MAX_REPLAY_BATCH_SIZE
        )
        self.allow_unsafe_ts_bootstrap: bool = kwargs.get(
            "allow_unsafe_ts_bootstrap", False
        )
        self.is_full_table_dump = False

        # Whether to use the server-native DUMP TABLE statement (FB-internal)
        self.use_dump_table_stmt: bool = kwargs.get("use_dump_table", False)
        # If using DUMP TABLE, controls the number of worker threads.
        self.dump_threads: int = kwargs.get("dump_threads", constant.DUMP_THREADS)

        self.replay_max_changes: int = kwargs.get(
            "replay_max_changes", constant.MAX_REPLAY_CHANGES
        )

        self.use_sql_wsenv: bool = kwargs.get("use_sql_wsenv", False)

        # checksum can have their own chunks
        self.checksum_chunk_size: int = kwargs.get(
            "chunk_size", constant.CHECKSUM_CHUNK_BYTES
        )

        if self.use_sql_wsenv:
            # by default, wsenv requires to use big chunk
            self.chunk_size: int = kwargs.get("chunk_size", constant.WSENV_CHUNK_BYTES)

            # by default, wsenv doesn't use local disk
            self.skip_disk_space_check: bool = kwargs.get("skip_disk_space_check", True)
            # skip local disk space check when using wsenv
            if not self.skip_disk_space_check:
                raise OSCError(
                    OSCError.Errors.SKIP_DISK_SPACE_CHECK_VALUE_INCOMPATIBLE_WSENV
                )

            # require outfile_dir not empty
            if not self.outfile_dir:
                raise OSCError(OSCError.Errors.OUTFILE_DIR_NOT_SPECIFIED_WSENV)
        else:
            self.chunk_size: int = kwargs.get("chunk_size", constant.CHUNK_BYTES)
            self.skip_disk_space_check: bool = kwargs.get(
                "skip_disk_space_check", False
            )

        self.enable_outfile_compression: bool = kwargs.get(
            "enable_outfile_compression", False
        )
        self.compressed_outfile_extension: str | None = kwargs.get(
            "compressed_outfile_extension", None
        )
        self.bulk_load_session_id: str | None = kwargs.get("bulk_load_session_id", None)
        self.max_id_now = 0
        self.mismatch_pk_charset: dict[str, str] = {}
        self.last_gc_collected = time.time()
        self.saved_table_timestamp: str = ""
        self.catchup_tool: OscCatchupTool = None

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
        It will be used to check whether the old schema has a primary key by
        comparing the length to zero. Also will be used in construct the
        condition part of the replay query
        """
        return [col.name for col in self._old_table.primary_key.column_list]

    @property
    def new_pk_list(self):
        """
        List of column names representing the primary key in
        the new schema.
        """
        return [col.name for col in self._new_table.primary_key.column_list]

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
        list of column names for all the columns in the old schema except the
        ones are being dropped in the new schema. Used to create triggers and
        delta table.
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
            col.name
            for col in self._old_table.column_list
            if col.name not in self._pk_for_filter
            and col.name not in self.dropped_column_name_list
        ]

    def checksum_column_list(self, exclude_pk: bool):
        """
        A list of column names suitable for comparing checksums. `exclude_pk`
        causes this function to exclude primary key columns, for use when the
        caller already provides them through another means.
        """
        column_list = []
        # Create a mapping from the new table's column names to their definitions
        # to detect changes to column definitions between old and new tables.
        new_columns = {col.name: col for col in self._new_table.column_list}
        old_pk_name_list = [c.name for c in self._old_table.primary_key.column_list]
        for col in self._old_table.column_list:
            # Filter out non-deterministically serialized column types.
            if col.column_type in constant.CHECKSUM_EXCLUDE_COLUMN_TYPES:
                continue
            if exclude_pk and col.name in old_pk_name_list:
                continue
            if col.name in self.dropped_column_name_list:
                continue
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
        if len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 10:
            return constant.DELTA_TABLE_PREFIX + self._old_table.name
        elif (
            len(self._old_table.name) >= constant.MAX_TABLE_LENGTH - 10
            and len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 2
        ):
            return constant.SHORT_DELTA_TABLE_PREFIX + self._old_table.name
        else:
            return constant.DELTA_TABLE_PREFIX + constant.GENERIC_TABLE_NAME

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
        if len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 10:
            return constant.NEW_TABLE_PREFIX + self.table_name
        elif (
            len(self._old_table.name) >= constant.MAX_TABLE_LENGTH - 10
            and len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 2
        ):
            return constant.SHORT_NEW_TABLE_PREFIX + self.table_name
        else:
            return constant.NEW_TABLE_PREFIX + constant.GENERIC_TABLE_NAME

    @property
    def renamed_table_name(self):
        """
        Name of the old table after swap.
        """
        if len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 10:
            return constant.RENAMED_TABLE_PREFIX + self._old_table.name
        elif (
            len(self._old_table.name) >= constant.MAX_TABLE_LENGTH - 10
            and len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 2
        ):
            return constant.SHORT_RENAMED_TABLE_PREFIX + self._old_table.name
        else:
            return constant.RENAMED_TABLE_PREFIX + constant.GENERIC_TABLE_NAME

    @property
    def insert_trigger_name(self):
        """
        Name of the "AFTER INSERT" trigger on the old table to capture changes
        during data dump/load
        """
        if len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 10:
            return constant.INSERT_TRIGGER_PREFIX + self._old_table.name
        elif (
            len(self._old_table.name) >= constant.MAX_TABLE_LENGTH - 10
            and len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 2
        ):
            return constant.SHORT_INSERT_TRIGGER_PREFIX + self._old_table.name
        else:
            return constant.INSERT_TRIGGER_PREFIX + constant.GENERIC_TABLE_NAME

    @property
    def update_trigger_name(self):
        """
        Name of the "AFTER UPDATE" trigger on the old table to capture changes
        during data dump/load
        """
        if len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 10:
            return constant.UPDATE_TRIGGER_PREFIX + self._old_table.name
        elif (
            len(self._old_table.name) >= constant.MAX_TABLE_LENGTH - 10
            and len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 2
        ):
            return constant.SHORT_UPDATE_TRIGGER_PREFIX + self._old_table.name
        else:
            return constant.UPDATE_TRIGGER_PREFIX + constant.GENERIC_TABLE_NAME

    @property
    def delete_trigger_name(self):
        """
        Name of the "AFTER DELETE" trigger on the old table to capture changes
        during data dump/load
        """
        if len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 10:
            return constant.DELETE_TRIGGER_PREFIX + self._old_table.name
        elif (
            len(self._old_table.name) >= constant.MAX_TABLE_LENGTH - 10
            and len(self._old_table.name) < constant.MAX_TABLE_LENGTH - 2
        ):
            return constant.SHORT_DELETE_TRIGGER_PREFIX + self._old_table.name
        else:
            return constant.DELETE_TRIGGER_PREFIX + constant.GENERIC_TABLE_NAME

    @property
    def outfile(self):
        """
        Full file path of the outfile for data dumping/loading. It's the prefix
        of outfile chunks. A single outfile chunk will look like
        '@datadir/__osc_tbl_@TABLE_NAME.@n'
        """
        return os.path.join(self.outfile_dir, constant.OUTFILE_TABLE + self.table_name)

    @property
    def tmp_table_exclude_id(self):
        """
        Name of the temporary table which contains the value of IDCOLNAME in
        self.delta_table_name which we've already replayed
        """
        return "__osc_temp_ids_to_exclude"

    @property
    def tmp_table_include_id(self):
        """
        Name of the temporary table which contains the value of IDCOLNAME in
        self.delta_table_name which we will be replaying for a single
        self.replay_changes() call
        """
        return "__osc_temp_ids_to_include"

    @property
    def outfile_exclude_id(self):
        """
        Name of the outfile which contains the data which will be loaded to
        self.tmp_table_exclude_id soon. We cannot use insert into select
        from, because that will hold gap lock inside transaction. The whole
        select into outfile/load data infile logic is a work around for this.
        """
        return os.path.join(
            self.outfile_dir, constant.OUTFILE_EXCLUDE_ID + self.table_name
        )

    @property
    def outfile_include_id(self):
        """
        Name of the outfile which contains the data which will be loaded to
        self.tmp_table_include_id soon. See docs in self.outfile_exclude_id
        for more
        """
        return os.path.join(
            self.outfile_dir, constant.OUTFILE_INCLUDE_ID + self.table_name
        )

    @property
    def droppable_indexes(self):
        """
        A list of lib.sqlparse.models objects representing the indexes which
        can be dropped before loading data into self.new_table_name to speed
        up data loading
        """
        # If we don't specify index recreation then just return a empty list
        # which stands for no index is suitable of dropping
        if not self.idx_recreation:
            return []
        # We need to keep unique index, if we need to use it to eliminate
        # duplicates during data loading
        return self._new_table.droppable_indexes(keep_unique_key=self.eliminate_dups)

    def _outfile_extension(self, skip_compressed_extension: bool = False) -> str:
        if (
            not skip_compressed_extension
            and self.enable_outfile_compression
            and self.compressed_outfile_extension
        ):
            return ".{}.{}".format(
                # NOTE: Do not use chunk size in compression
                #       This is intentional because we want to be able to predictably
                #       determine the exact file that mysqld would create
                #       (such as `{filename}.{mysqld_chunk_number}.{extension}`)
                #       and because OSC does already do chunking in the not
                #       compressed path
                0,  # mysql_chunk_number is always 0
                self.compressed_outfile_extension,
            )
        else:
            return ""

    def _outfile_name(
        self,
        chunk_id: int,
        suffix: Optional[str] = None,
        skip_compressed_extension: bool = False,
    ) -> str:
        return "{}{}.{}{}".format(
            self.outfile,
            suffix or "",
            chunk_id,
            self._outfile_extension(
                skip_compressed_extension=skip_compressed_extension
            ),
        )

    def set_tx_isolation(self):
        """
        Setting the session isolation level to RR for OSC
        """
        # https://dev.mysql.com/worklog/task/?id=9636
        # MYSQL_5_TO_8_MIGRATION
        if self.mysql_version.is_mysql8:
            self.execute_sql(
                sql.set_session_variable("transaction_isolation"), ("REPEATABLE-READ",)
            )
        else:
            self.execute_sql(
                sql.set_session_variable("tx_isolation"), ("REPEATABLE-READ",)
            )

    def set_sql_mode(self):
        """
        Setting the sql_mode to STRICT for the connection we will using for OSC
        """
        self.execute_sql(
            sql.set_session_variable("sql_mode"),
            ("STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO",),
        )

    def parse_session_overrides_str(self, overrides_str):
        """
        Given a session overrides string, break it down to a list of overrides

        @param overrides_str:  A plain string that contains the overrides
        @type  overrides_str:  string

        @return : A list of [var, value]
        """
        overrides = []
        if overrides_str is None or overrides_str == "":
            return []
        for section in overrides_str.split(";"):
            splitted_array = section.split("=")
            if (
                len(splitted_array) != 2
                or splitted_array[0] == ""
                or splitted_array[1] == ""
            ):
                raise OSCError(
                    OSCError.Errors.INCORRECT_SESSION_OVERRIDE, {"section": section}
                )
            overrides.append(splitted_array)
        return overrides

    def override_session_vars(self):
        """
        Override session variable if there's any
        """
        self.session_overrides = self.parse_session_overrides_str(
            self.session_overrides_str
        )
        for var_name, var_value in self.session_overrides:
            log.info(
                "Override session variable {} with value: {}".format(
                    var_name, var_value
                )
            )
            self.execute_sql(sql.set_session_variable(var_name), (var_value,))

    def is_var_enabled(self, var_name):
        if var_name not in self.mysql_vars:
            return False
        if self.mysql_vars[var_name] == "OFF":
            return False
        if self.mysql_vars[var_name] == "0":
            return False
        return True

    @property
    def is_trigger_rbr_safe(self):
        """
        Only fb-mysql is safe for RBR if we create trigger on master alone
        Otherwise slave will hit _chg table not exists error
        """
        # We only need to check this if RBR is enabled
        if self.mysql_vars["binlog_format"] == "ROW":
            return not self.is_var_enabled("sql_log_bin_triggers")
        else:
            return True

    @property
    def is_myrocks_table(self):
        if not self._new_table.engine:
            return False
        return self._new_table.engine.upper() == "ROCKSDB"

    @property
    def is_myrocks_ttl_table(self):
        return self._new_table.is_myrocks_ttl_table

    def sanity_checks(self):
        """
        Check MySQL setting for requirements that we don't necessarily need to
        hold a name lock for
        """
        if not self.is_trigger_rbr_safe:
            raise OSCError(OSCError.Errors.NOT_RBR_SAFE)

    def skip_cache_fill_for_myrocks(self):
        """
        Skip block cache fill for dumps and scans to avoid cache pollution
        """
        if "rocksdb_skip_fill_cache" in self.mysql_vars:
            self.execute_sql(sql.set_session_variable("rocksdb_skip_fill_cache"), (1,))

    def table_timestamp_change_on_truncation_is_available(self):
        try:
            result = self.query_variable(
                "update_table_create_timestamp_on_truncate", "global"
            )
        except MySQLdb.MySQLError:
            return False
        return bool(result)

    def record_table_timestamp(self):
        if self.table_timestamp_change_on_truncation_is_available():
            self.execute_sql(
                sql.set_global_variable("update_table_create_timestamp_on_truncate"),
                ("ON",),
            )
        self.saved_table_timestamp = self.get_table_timestamp()
        log.info(
            "Saved table create timestamp {} for table {}.".format(
                self.saved_table_timestamp, self.table_name
            )
        )

    def stop_tracking_table_timestamp(self):
        if self.table_timestamp_change_on_truncation_is_available():
            self.execute_sql(
                sql.set_global_variable("update_table_create_timestamp_on_truncate"),
                ("OFF",),
            )

    def stop_if_table_timestamp_changed(func):
        """
        A decorator to check table timestamp has changed before executing a step
        in the copy workflow. If the table timestamp has changed, it aborts by
        raising an exception.
        """

        def before(self, *args, **kwargs):
            previous_table_timestamp = self.saved_table_timestamp
            new_table_timestamp = self.get_table_timestamp()
            if previous_table_timestamp != new_table_timestamp:
                raise OSCError(
                    OSCError.Errors.TABLE_TIMESTAMP_CHANGED_ERROR,
                    {"expected": previous_table_timestamp, "got": new_table_timestamp},
                )
            log.info("No table timestamp change detected.")
            return func(self, *args, **kwargs)

        return before

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
        self.enable_priority_ddl()
        self.skip_cache_fill_for_myrocks()
        self.enable_sql_wsenv()
        self.override_session_vars()
        self.get_osc_lock()

    def table_exists(self, table_name):
        """
        Given a table_name check whether this table already exist under
        current working database

        @param table_name:  Name of the table to check existence
        @type  table_name:  string
        """
        table_exists = self.query(
            sql.table_existence,
            (
                table_name,
                self._current_db,
            ),
        )
        return bool(table_exists)

    def fetch_table_schema(self, table_name):
        """
        Use lib.sqlparse.parse_create to turn a CREATE TABLE syntax into a
        TABLE object, so that we can then do stuff in a pythonic way later.
        """
        ddl = self.query(sql.show_create_table(table_name))
        if ddl:
            try:
                return self.parse_function(ddl[0]["Create Table"], self.use_ast_parser)
            except ParseError as e:
                raise OSCError(
                    OSCError.Errors.TABLE_PARSING_ERROR,
                    {"db": self._current_db, "table": self.table_name, "msg": str(e)},
                )

    def fetch_partitions(self, table_name):
        """
        Fetching partition names from information_schema. This will be used
        when dropping table. If a table has a partition schema, then its
        partition will be dropped one by one before the table get dropped.
        This way we will bring less pressure to the MySQL server
        """
        partition_result = self.query(
            sql.fetch_partition,
            (
                self._current_db,
                table_name,
            ),
        )
        # If a table doesn't have partition schema the "PARTITION_NAME"
        # will be string "None" instead of something considered as false
        # in python
        return [
            partition_entry["PARTITION_NAME"]
            for partition_entry in partition_result
            if partition_entry["PARTITION_NAME"] != "None"
        ]

    @wrap_hook
    def swap_table_block(self):
        self.stats["swap_table_block"] = (
            "Waiting on periodic database backup to complete. ETA: 3 hours."
        )
        return

    @retryable(num_tries=3)
    def init_table_obj(self):
        """
        Instantiate self._old_table by parsing the output of SHOW CREATE
        TABLE from MySQL instance. Because we need to parse out the table name
        we'll act on, this should be the first step before we start to doing
        anything
        """
        # Check the existence of original table
        if not self.table_exists(self.table_name):
            raise OSCError(
                OSCError.Errors.TABLE_NOT_EXIST,
                {"db": self._current_db, "table": self.table_name},
            )
        self._old_table = self.fetch_table_schema(self.table_name)
        self.partitions[self.table_name] = self.fetch_partitions(self.table_name)
        # The table after swap will have the same partition layout as current
        # table
        self.partitions[self.renamed_table_name] = self.partitions[self.table_name]
        # Preserve the auto_inc value from old table, so that we don't revert
        # back to a smaller value after OSC
        if self._old_table.auto_increment:
            self._new_table.auto_increment = self._old_table.auto_increment
        # We don't change the storage engine in OSC, so just use
        # the fetched instance storage engine
        self._new_table.engine = self._old_table.engine
        # Populate both old and new tables with explicit charset/collate
        self.populate_charset_collation(self._old_table)
        self.populate_charset_collation(self._new_table)

    def cleanup_with_force(self):
        """
        Loop through all the tables we will touch during OSC, and clean them
        up if force_cleanup is specified
        """
        log.info(
            "--force-cleanup specified, cleaning up things that may left "
            "behind by last run"
        )
        cleanup_payload = CleanupPayload(
            charset=self.charset,
            sudo=self.sudo,
            disable_replication=self.disable_replication,
        )
        # cleanup outfiles for include_id and exclude_id
        for filepath in (self.outfile_exclude_id, self.outfile_include_id):
            cleanup_payload.add_file_entry(filepath)
        # cleanup outfiles for detailed checksum
        cleanup_payload.add_file_entry(
            "{}*".format(
                self._outfile_name(
                    suffix=".old", chunk_id=0, skip_compressed_extension=True
                )
            )
        )
        cleanup_payload.add_file_entry(
            "{}*".format(
                self._outfile_name(
                    suffix=".new", chunk_id=0, skip_compressed_extension=True
                )
            )
        )
        # cleanup outfiles for table dump
        file_prefixes = [
            self.outfile,
            "{}.old".format(self.outfile),
            "{}.new".format(self.outfile),
        ]
        for file_prefix in file_prefixes:
            log.debug("globbing {}".format(file_prefix))
            for outfile in glob.glob(
                "{}.[0-9]*".format(file_prefix),
            ):
                cleanup_payload.add_file_entry(outfile)
        for trigger in (
            self.delete_trigger_name,
            self.update_trigger_name,
            self.insert_trigger_name,
        ):
            cleanup_payload.add_drop_trigger_entry(self._current_db, trigger)
        for tbl in (
            self.new_table_name,
            self.delta_table_name,
            self.renamed_table_name,
        ):
            partitions = self.fetch_partitions(tbl)
            cleanup_payload.add_drop_table_entry(self._current_db, tbl, partitions)
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
        for var_name in ("@@secure_file_priv", "@@datadir"):
            result = self.query(sql.select_as(var_name, "folder"))
            if not result:
                raise Exception("Failed to get {} system variable".format(var_name))
            if result[0]["folder"]:
                if var_name == "@@secure_file_priv":
                    self.outfile_dir = result[0]["folder"]
                else:
                    self.outfile_dir = os.path.join(
                        result[0]["folder"], self._current_db_dir
                    )
                log.info("Will use {} storing dump outfile".format(self.outfile_dir))
                return
        raise Exception("Cannot determine output dir for dump")

    def table_check(self):
        tables_to_check = (
            self.new_table_name,
            self.delta_table_name,
            self.renamed_table_name,
        )
        for table_name in tables_to_check:
            if self.table_exists(table_name):
                raise OSCError(
                    OSCError.Errors.TABLE_ALREADY_EXIST,
                    {"db": self._current_db, "table": table_name},
                )

        # Make sure new table schema has primary key
        if not all(
            (self._new_table.primary_key, self._new_table.primary_key.column_list)
        ):
            raise OSCError(
                OSCError.Errors.NO_PK_EXIST,
                {"db": self._current_db, "table": self.table_name},
            )

    def trigger_check(self):
        """
        Check whether there's any trigger already exist on the table we're
        about to touch
        """
        triggers = self.query(
            sql.trigger_existence,
            (self.table_name, self._current_db),
        )
        if triggers:
            trigger_desc = [
                "Trigger name: {}, Action: {} {}".format(
                    trigger["TRIGGER_NAME"],
                    trigger["ACTION_TIMING"],
                    trigger["EVENT_MANIPULATION"],
                )
                for trigger in triggers
            ]
            raise OSCError(
                OSCError.Errors.TRIGGER_ALREADY_EXIST,
                {"triggers": "\n".join(trigger_desc)},
            )

    def foreign_key_check(self):
        """
        Check whether the table has been referred to any existing foreign
        definition
        """
        # MyRocks doesn't support foreign key
        if self.is_myrocks_table:
            log.info(
                "Skip foreign key check because MyRocks doesn't support " "this yet"
            )
            return True
        foreign_keys = self.query(
            sql.foreign_key_cnt,
            (
                self.table_name,
                self._current_db,
                self.table_name,
                self._current_db,
            ),
        )
        if foreign_keys:
            fk = "CONSTRAINT `{}` FOREIGN KEY (`{}`) REFERENCES `{}` (`{}`)".format(
                foreign_keys[0]["constraint_name"],
                foreign_keys[0]["col_name"],
                foreign_keys[0]["ref_tab"],
                foreign_keys[0]["ref_col_name"],
            )
            raise OSCError(
                OSCError.Errors.FOREIGN_KEY_FOUND,
                {"db": self._current_db, "table": self.table_name, "fk": fk},
            )

    def get_table_size_from_IS(self, table_name):
        """
        Given a table_name return its current size in Bytes from
        information_schema

        @param table_name:  Name of the table to fetch size
        @type  table_name:  string
        """
        result = self.query(sql.show_table_stats(self._current_db), (self.table_name,))
        if result:
            record = result[0]
            table_size = record["Data_length"] + record["Index_length"]
            log.info(
                f"Table {self.table_name} size: {table_size} "
                f"(data {record['Data_length']}, index {record['Index_length']}), "
                f"rows: {record['Rows']}"
            )
            return table_size
        return 0

    def get_table_size_for_myrocks(self, table_name):
        """
        Given a table_name return its raw data size before compression.
        MyRocks is very good at compression, the on disk dump size
        is much bigger than the actual MyRocks table size, hence we will
        use raw size for the estimation of the maximum disk usage

        @param table_name:  Name of the table to fetch size
        @type  table_name:  string
        """
        result = self.query(
            sql.get_myrocks_table_dump_size(),
            (
                self._current_db,
                self.table_name,
            ),
        )

        if result:
            log.info(f"MyRocks uncompressed PK size: {result[0]['raw_size']}")
            return result[0]["raw_size"] or 0
        return 0

    def get_table_size(self, table_name):
        """
        Given a table_name return its current size in Bytes

        @param table_name:  Name of the table to fetch size
        @type  table_name:  string
        """
        # Size of the new table on disk including all indexes.
        # In MyRocks it could be compressed.
        return self.get_table_size_from_IS(table_name)

    def get_expected_compression_ratio_pct(self) -> int:
        """
        Return expected compression ratio pct for new table.
        """
        return 100

    def get_expected_dump_size(self, table_name):
        """
        Given a table_name return its expected outfile size in Bytes.

        @param table_name:  Name of the table to fetch size
        @type  table_name:  string
        """
        on_disk_size = self.get_table_size(table_name)

        # Figure out the dump data size and adjust for compression.
        dump_size = on_disk_size
        if self.is_myrocks_table:
            dump_size = self.get_table_size_for_myrocks(table_name)

        if self.enable_outfile_compression:
            dump_size *= self.get_expected_compression_ratio_pct()
            dump_size //= 100  # Perform integer floor division.

        return dump_size

    def check_disk_size(self):
        """
        Check if we have enough disk space to execute the DDL
        """
        self.table_size = int(self.get_table_size(self.table_name))
        if self.skip_disk_space_check:
            return True

        dump_size = int(self.get_expected_dump_size(self.table_name))
        disk_space = int(util.disk_partition_free(self.outfile_dir))
        # With allow_new_pk, we will create one giant outfile, and so at
        # some point will have the entire new table and the entire outfile
        # both existing simultaneously.
        if self.allow_new_pk and not self._old_table.primary_key.column_list:
            required_size = self.table_size + dump_size
        else:
            # Dump chunks are deleted as they are loaded into new table.
            # Take the max of table and dump size and add 10% just in case.
            required_size = max(self.table_size, dump_size) * 1.1
        log.info(
            "Disk space required: {}, available: {}".format(
                util.readable_size(required_size), util.readable_size(disk_space)
            )
        )
        if required_size > disk_space:
            raise OSCError(
                OSCError.Errors.NOT_ENOUGH_SPACE,
                {
                    "need": util.readable_size(required_size),
                    "avail": util.readable_size(disk_space),
                },
            )

    def check_disk_free_space_reserved(self):
        """
        Check if we have enough free space left during dump data
        """
        if self.skip_disk_space_check:
            return True
        disk_partition_size = util.disk_partition_size(self.outfile_dir)
        free_disk_space = util.disk_partition_free(self.outfile_dir)
        free_space_factor = self.free_space_reserved_percent / 100
        free_space_reserved = disk_partition_size * free_space_factor
        if free_disk_space < free_space_reserved:
            raise OSCError(
                OSCError.Errors.NOT_ENOUGH_SPACE,
                {
                    "need": util.readable_size(free_space_reserved),
                    "avail": util.readable_size(free_disk_space),
                },
            )

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
        PK for row searching.
        Same for old PK being (a, b, c), new PK is (a, b) because new PK is more
        strict, so it will always return at most one row when using old PK columns
        as WHERE condition.
        However if the old PK is (a, b, c), new PK is (b, c, d). Then there's
        a chance the changes may not be able to be replay efficiently. Because
        using only column (b, c) for row searching may result in a huge number
        of matched rows
        """
        idx_on_new_table = [self._new_table.primary_key] + self._new_table.indexes
        old_pk_len = len(self._pk_for_filter)
        for idx in idx_on_new_table:
            log.debug("Checking prefix for {}".format(idx.name))
            idx_prefix = idx.column_list[:old_pk_len]
            idx_name_set = {col.name for col in idx_prefix}
            # Identical set and covered set are considered as covering
            if set(self._pk_for_filter) == idx_name_set:
                log.info("PK prefix on new table can cover PK from old table")
                return True
            if idx.is_unique and set(self._pk_for_filter) > idx_name_set:
                log.info("old PK can uniquely identify rows from new schema")
                return True

        return False

    def find_coverage_index(self):
        """
        Find an unique index which can perfectly cover old pri-key search in
        order to calculate checksum for new table. We will use this index name
        as force index in checksum query
        See validate_post_alter_pk for more detail about pri-key coverage
        """
        idx_on_new_table = [self._new_table.primary_key] + self._new_table.indexes
        old_pk_len = len(self._pk_for_filter)
        for idx in idx_on_new_table:
            # list[:idx] where idx > len(list) yields full list
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
            self.range_start_vars_array.append("@range_start_{}".format(idx))
            self.range_end_vars_array.append("@range_end_{}".format(idx))
        self.range_start_vars = ",".join(self.range_start_vars_array)
        self.range_end_vars = ",".join(self.range_end_vars_array)

    def make_chunk_size_odd(self):
        """
        Ensure select_chunk_size is an odd number. If a column has exact the same
        value for all its rows, then return value from BIT_XOR(CRC32(`col`))
        will be zero for even number of rows, no matter what value it has.
        """
        if self.select_checksum_chunk_size % 2 == 0:
            self.select_checksum_chunk_size = self.select_checksum_chunk_size + 1

        # TODO: not needed ideally, right now. But will leave it.
        if self.select_chunk_size % 2 == 0:
            self.select_chunk_size = self.select_chunk_size + 1

    def get_table_chunk_size(self):
        """
        Calculate the number of rows for each table dump query table based on
        average row length and the chunks size we've specified
        """
        result = self.query(
            sql.table_avg_row_len,
            (
                self._current_db,
                self.table_name,
            ),
        )

        if (
            self.is_myrocks_table
            and self.get_bulk_load_parameters().should_disable_bulk_load
        ):
            self.chunk_size = constant.CHUNK_BYTES
            self.checksum_chunk_size = constant.CHUNK_BYTES
            log.info(f"reduce the chunk size: {constant.CHUNK_BYTES}.")

        if result:
            tbl_avg_length = result[0]["AVG_ROW_LENGTH"]
            # avoid huge chunk row count
            if tbl_avg_length < 20:
                tbl_avg_length = 20
            self.select_chunk_size = self.chunk_size // tbl_avg_length

            # This means either the avg row size is huge, or user specified
            # a tiny select_chunk_size on CLI. Let's make it one row per
            # outfile to avoid zero division
            if not self.select_chunk_size:
                self.select_chunk_size = 1

            self.select_checksum_chunk_size = self.checksum_chunk_size // tbl_avg_length
            if not self.select_checksum_chunk_size:
                self.select_checksum_chunk_size = 1

            log.info(
                "Table contains {} rows, data size: {}, index size: {} "
                "(total size: {}), table_avg_row_len: {} bytes,"
                "chunk_size: {} bytes, checksum chunk_size {} bytes.".format(
                    result[0]["TABLE_ROWS"],
                    result[0]["DATA_LENGTH"],
                    result[0]["INDEX_LENGTH"],
                    result[0]["DATA_LENGTH"] + result[0]["INDEX_LENGTH"],
                    tbl_avg_length,
                    self.chunk_size,
                    self.checksum_chunk_size,
                )
            )
            log.info(
                "Outfile will contain {} rows each.".format(self.select_chunk_size)
            )
            self.eta_chunks = max(
                int(result[0]["TABLE_ROWS"] / self.select_chunk_size), 1
            )
        else:
            raise OSCError(OSCError.Errors.FAIL_TO_GUESS_CHUNK_SIZE)

    def has_desired_schema(self):
        """
        Check whether the existing table already has the desired schema.
        """
        if self._new_table == self._old_table:
            if not self.rebuild:
                log.info("Table already has the desired schema. ")
                return True
            else:
                log.info(
                    "Table already has the desired schema. However "
                    "--rebuild is specified, doing a rebuild instead"
                )
                return False
        return False

    def decide_pk_for_filter(self):
        # If we are adding a PK, then we should use all the columns in
        # old table to identify an unique row
        all_col_def = {col.name: col for col in self._old_table.column_list}
        if not all(
            (self._old_table.primary_key, self._old_table.primary_key.column_list)
        ):
            # Let's try to get an UK if possible
            for idx in self._old_table.indexes:
                if idx.is_unique:
                    log.info(
                        "Old table doesn't have a PK but has an UK: {}".format(idx.name)
                    )
                    self._pk_for_filter = [col.name for col in idx.column_list]
                    self._idx_name_for_filter = idx.name
                    break
            else:
                # There's no UK either
                if self.allow_new_pk:
                    self._pk_for_filter = [
                        col.name for col in self._old_table.column_list
                    ]
                    self.is_full_table_dump = True
                else:
                    raise OSCError(OSCError.Errors.NEW_PK)
        # If we have PK in existing schema, then we use current PK as an unique
        # row finder
        else:
            # if any of the columns of the primary key is prefixed, we want to
            # use full_table_dump, instead of chunking, so that it doesn't fill
            # up the disk
            # e.g. name below is a prefixed col in the PK (assume varchar(99))
            # since we dont use full col in PK - `PRIMARY KEY(id, name(10))`
            for col in self._old_table.primary_key.column_list:
                if col.length:
                    log.info(
                        "Found prefixed column/s as part of the PK. "
                        "Will do full table dump (no chunking)."
                    )
                    self._pk_for_filter = [c.name for c in self._old_table.column_list]
                    self.is_full_table_dump = True
                    break
            else:
                self._pk_for_filter = [
                    col.name for col in self._old_table.primary_key.column_list
                ]
        self._pk_for_filter_def = [
            all_col_def[col_name] for col_name in self._pk_for_filter
        ]

    def ts_bootstrap_check(self):
        """
        Check when going from old schema to new, whether bootstraping column using
        CURRENT_TIMESTAMP is involved. This is a dangerous thing to do out of
        replication and is disallowed by default
        """
        if not need_default_ts_bootstrap(self._old_table, self._new_table):
            return
        if self.allow_unsafe_ts_bootstrap:
            log.warning(
                "Bootstraping timestamp column using current time is required. "
                "Bypassing the safety check as requested"
            )
            return
        raise OSCError(OSCError.Errors.UNSAFE_TS_BOOTSTRAP)

    @wrap_hook
    def pre_osc_check(self):
        """
        Pre-OSC sanity check.
        Make sure all temporary table which will be used during data copy
        stage doesn't exist before we actually creating one.
        Also doing some index sanity check.
        """
        # Make sure temporary table we will use during copy doesn't exist
        self.table_check()
        self.decide_pk_for_filter()

        # Check if we can have indexes in new table to efficiently look up
        # current old pk combinations
        if not self.validate_post_alter_pk():
            self.table_size = self.get_table_size(self.table_name)
            if self.skip_pk_coverage_check:
                log.warning(
                    "Indexes on new table cannot cover current PK of "
                    "the old schema, which will make binary logs replay "
                    "in an inefficient way."
                )
            elif self.table_size < self.pk_coverage_size_threshold:
                log.warning(
                    "No index on new table can cover old pk. Since this is "
                    "a small table: {}, we fallback to a full table dump".format(
                        self.table_size
                    )
                )
                # All columns will be chosen if we are dumping table without
                # chunking, this means all columns will be used as a part of
                # the WHERE condition when replaying
                self.is_full_table_dump = True
                self._pk_for_filter = [col.name for col in self._old_table.column_list]
                self._pk_for_filter_def = self._old_table.column_list.copy()
            elif self.is_full_table_dump:
                log.warning(
                    "Skipping coverage index test, since we are doing "
                    "full table dump"
                )
            else:
                old_pk_names = ", ".join(
                    "`{}`".format(col.name)
                    for col in self._old_table.primary_key.column_list
                )
                raise OSCError(
                    OSCError.Errors.NO_INDEX_COVERAGE, {"pk_names": old_pk_names}
                )

        log.info(
            "PK filter for replaying changes later: {}".format(self._pk_for_filter)
        )

        self.foreign_key_check()
        self.trigger_check()
        self.init_range_variables()
        self.get_table_chunk_size()
        self.make_chunk_size_odd()
        self.check_disk_size()
        self.ts_bootstrap_check()
        self.drop_columns_check()

        # Check things that require myrocks
        if not self.is_myrocks_table:
            if self.use_dump_table_stmt:
                raise OSCError(
                    OSCError.Errors.MYROCKS_REQUIRED, {"reason": "DUMP TABLE statement"}
                )

    def drop_columns_check(self):
        # We only allow dropping columns with the flag --allow-drop-column.
        if self.dropped_column_name_list:
            if self.allow_drop_column:
                for diff_column in self.dropped_column_name_list:
                    log.warning(
                        "Column `{}` is missing in the new schema, "
                        "but --allow-drop-column is specified. Will "
                        "drop this column.".format(diff_column)
                    )
            else:
                missing_columns = ", ".join(self.dropped_column_name_list)
                raise OSCError(
                    OSCError.Errors.MISSING_COLUMN, {"column": missing_columns}
                )
            # We don't allow dropping columns from current primary key
            for col in self._pk_for_filter:
                if col in self.dropped_column_name_list:
                    raise OSCError(OSCError.Errors.PRI_COL_DROPPED, {"pri_col": col})

    def add_drop_table_entry(self, table_name):
        """
        A wrapper for adding drop table request to CleanupPayload.
        The database name will always be the one we are currently working on.
        Also partition name list will be included as fetched from information
        schema before DDL
        """
        self._cleanup_payload.add_drop_table_entry(
            self._current_db, table_name, self.partitions.get(table_name, [])
        )

    def get_collations(self):
        """
        Get a list of supported collations with their corresponding charsets
        """
        collations = self.query(sql.all_collation)
        collation_charsets = {}
        for r in collations:
            collation_charsets[r["COLLATION_NAME"]] = r["CHARACTER_SET_NAME"]
        return collation_charsets

    def get_default_collations(self):
        """
        Get a list of supported character set and their corresponding default
        collations
        """
        collations = self.query(sql.default_collation)
        charset_collations = {}
        for r in collations:
            charset_collations[r["CHARACTER_SET_NAME"]] = r["COLLATION_NAME"]

        # Populate utf8mb4 override
        utf8_override = self.query(
            sql.get_global_variable("default_collation_for_utf8mb4")
        )
        if utf8_override and "utf8mb4" in charset_collations:
            charset_collations["utf8mb4"] = utf8_override[0]["Value"]
        if "utf8" not in charset_collations and "utf8mb3" in charset_collations:
            charset_collations["utf8"] = charset_collations["utf8mb3"]
        return charset_collations

    def populate_charset_collation(self, schema_obj):
        default_collations = self.get_default_collations()
        collation_charsets = self.get_collations()
        if schema_obj.charset is not None and schema_obj.collate is None:
            # "utf8" and "utf8mb3" are alias for table charset
            # since 8.0.32, utf8 is no longer available in the default
            # collations.
            if schema_obj.charset == "utf8mb3":
                schema_obj.collate = default_collations.get("utf8", None)
            else:
                schema_obj.collate = default_collations.get(schema_obj.charset, None)
        if schema_obj.charset is None and schema_obj.collate is not None:
            # Shouldn't reach here, since every schema should have default charset,
            # otherwise linting will error out. Leave the logic here just in case.
            # In this case, we would not populate the charset because we actually
            # want the user to explicit write the charset in the desired schema.
            # In db, charset is always populated(explicit) by default.
            schema_obj.charset = None

        # make column charset & collate explicit
        # follow https://dev.mysql.com/doc/refman/8.0/en/charset-column.html
        text_types = {"CHAR", "VARCHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM"}
        for column in schema_obj.column_list:
            if column.column_type in text_types:
                # Check collate first to guarantee the column uses table collate
                # if column charset is absent. If checking charset first and column
                # collate is absent, it will use table charset and get default
                # collate from the database, which does not work for tables with
                # non default collate settings
                if column.collate is None:
                    if column.charset and default_collations.get(column.charset, None):
                        column.collate = default_collations[column.charset]
                    else:
                        column.collate = schema_obj.collate
                if column.charset is None:
                    if column.collate and collation_charsets.get(column.collate, None):
                        column.charset = collation_charsets[column.collate]
                    else:
                        # shouldn't reach here, unless charset_to_collate
                        # or collate_to_charset doesn't have the mapped value
                        column.charset = schema_obj.charset
        return schema_obj

    def remove_using_hash_for_80(self):
        """
        Remove `USING HASH` for indexes that explicitly have it, because that's
        the 8.0 behavior
        """
        for index in self._new_table.indexes:
            if index.using == "HASH":
                index.using = None

    @wrap_hook
    def create_copy_table(self):
        """
        Create the physical temporary table using new schema
        """
        tmp_sql_obj = deepcopy(self._new_table)
        tmp_sql_obj.name = self.new_table_name
        if self.rm_partition:
            tmp_sql_obj.partition = self._old_table.partition
            tmp_sql_obj.partition_config = self._old_table.partition_config
        tmp_table_ddl = tmp_sql_obj.to_sql()
        log.info("Creating copy table using: {}".format(tmp_table_ddl))
        self.execute_sql(tmp_table_ddl)
        self.partitions[self.new_table_name] = self.fetch_partitions(
            self.new_table_name
        )
        self.add_drop_table_entry(self.new_table_name)

        # Check whether the schema is consistent after execution to avoid
        # any implicit conversion
        if self.fail_for_implicit_conv:
            obj_after = self.fetch_table_schema(self.new_table_name)
            obj_after.name = self._new_table.name
            # Ignore partition difference, since there will be no implicit
            # conversion here
            obj_after.partition = self._new_table.partition
            obj_after.partition_config = self._new_table.partition_config
            self.populate_charset_collation(obj_after)
            if self.mysql_version.is_mysql8:
                # Remove 'USING HASH' in keys on 8.0, when present in 5.6, as 8.0
                # removes it by default
                self.remove_using_hash_for_80()
            if self.is_myrocks_table:
                log.warning(
                    f"Ignore BTREE indexes in table `{self._new_table.name}` on RocksDB"
                )
                for idx in self._new_table.indexes:
                    if idx.using == "BTREE":
                        idx.using = None
            if obj_after != self._new_table:
                raise OSCError(
                    OSCError.Errors.IMPLICIT_CONVERSION_DETECTED,
                    {"diff": str(SchemaDiff(self._new_table, obj_after))},
                )

    @wrap_hook
    def create_delta_table(self):
        """
        Create the table which will store changes made to existing table during
        OSC. This can be considered as table level binlog
        """
        self.execute_sql(
            sql.create_delta_table(
                self.delta_table_name,
                self.IDCOLNAME,
                self.DMLCOLNAME,
                self._old_table.engine,
                self.old_column_list,
                self._old_table.name,
            )
        )
        self.add_drop_table_entry(self.delta_table_name)
        # We will break table into chunks when calculate checksums using
        # old primary key. We need this index to skip verify the same row
        # for multiple time if it has been changed a lot
        if self._pk_for_filter_def and not self.is_full_table_dump:
            self.execute_sql(
                sql.create_idx_on_delta_table(
                    self.delta_table_name,
                    [col.name for col in self._pk_for_filter_def],
                )
            )

    def create_insert_trigger(self):
        self.execute_sql(
            sql.create_insert_trigger(
                self.insert_trigger_name,
                self.table_name,
                self.delta_table_name,
                self.DMLCOLNAME,
                self.old_column_list,
                self.DML_TYPE_INSERT,
            )
        )
        self._cleanup_payload.add_drop_trigger_entry(
            self._current_db, self.insert_trigger_name
        )

    @wrap_hook
    def create_delete_trigger(self):
        self.execute_sql(
            sql.create_delete_trigger(
                self.delete_trigger_name,
                self.table_name,
                self.delta_table_name,
                self.DMLCOLNAME,
                self.old_column_list,
                self.DML_TYPE_DELETE,
            )
        )
        self._cleanup_payload.add_drop_trigger_entry(
            self._current_db, self.delete_trigger_name
        )

    def create_update_trigger(self):
        self.execute_sql(
            sql.create_update_trigger(
                self.update_trigger_name,
                self.table_name,
                self.delta_table_name,
                self.DMLCOLNAME,
                self.old_column_list,
                self.DML_TYPE_UPDATE,
                self.DML_TYPE_DELETE,
                self.DML_TYPE_INSERT,
                self._pk_for_filter,
            )
        )
        self._cleanup_payload.add_drop_trigger_entry(
            self._current_db, self.update_trigger_name
        )

    def get_long_trx(self):
        """
        Return a long running transaction against the table we'll touch,
        if there's one.
        This is mainly for safety as long running transaction may block DDL,
        thus blocks more other requests
        """
        if self.skip_long_trx_check:
            return False
        processes = self.query(sql.show_processlist)
        for proc in processes:
            if not proc["Info"]:
                sql_statement = ""
            else:
                if isinstance(proc["Info"], bytes):
                    sql_statement = proc["Info"].decode("utf-8", "replace")
                else:
                    sql_statement = proc["Info"]

            proc["Info"] = sql_statement
            # Time can be None if the connection is in "Connect" state
            if (
                (proc.get("Time") or 0) > self.long_trx_time
                and proc.get("db", "") == self._current_db
                and self.table_name in "--" + sql_statement
                and not proc.get("Command", "") == "Sleep"
            ):
                return proc

    def wait_until_slow_query_finish(self):
        for _ in range(self.max_wait_for_slow_query):
            slow_query = self.get_long_trx()
            if slow_query:
                log.info(
                    "Slow query pid={} is still running".format(slow_query.get("Id", 0))
                )
                time.sleep(5)
            else:
                return True
        else:
            raise OSCError(
                OSCError.Errors.LONG_RUNNING_TRX,
                {
                    "pid": slow_query.get("Id", 0),
                    "user": slow_query.get("User", ""),
                    "host": slow_query.get("Host", ""),
                    "time": slow_query.get("Time", ""),
                    "command": slow_query.get("Command", ""),
                    "info": slow_query.get("Info", b"")
                    .encode("utf-8")
                    .decode("utf-8", "replace"),
                },
            )

    def kill_selects(self, table_names, conn=None):
        """
        Kill current running SELECTs against the specified tables in the
        working database so that they won't block the DDL statement we're
        about to execute. The conn parameter allows to use a different
        connection. A different connection is necessary when it is needed to
        kill queries that may be blocking the current connection
        """
        conn = conn or self.conn
        table_names = [tbl.lower() for tbl in table_names]

        # We use regex matching to find running queries on top of the tables
        # Better options (as in more precise) would be:
        # 1. List the current held metadata locks, but this is not possible
        #    without the performance schema
        # 2. Actually parse the SQL of the running queries, but this can be
        #    quite expensive
        keyword_pattern = (
            r"(\s|^)"  # whitespace or start
            r"({})"  # keyword(s)
            r"(\s|$)"  # whitespace or end
        )
        table_pattern = (
            r"(\s|`)"  # whitespace or backtick
            r"({})"  # table(s)
            r"(\s|`|$)"  # whitespace, backtick or end
        )
        alter_or_select_pattern = re.compile(keyword_pattern.format("select|alter"))
        information_schema_pattern = re.compile(
            keyword_pattern.format("information_schema")
        )
        any_tables_pattern = re.compile(table_pattern.format("|".join(table_names)))

        processlist = conn.get_running_queries()
        for proc in processlist:
            sql_statement = proc.get("Info") or "".encode("utf-8")
            sql_statement = sql_statement.decode("utf-8", "replace").lower()

            if (
                proc["db"] == self._current_db
                and sql_statement
                and not information_schema_pattern.search(sql_statement)
                and any_tables_pattern.search(sql_statement)
                and alter_or_select_pattern.search(sql_statement)
            ):
                try:
                    conn.kill_query_by_id(int(proc["Id"]))
                except MySQLdb.MySQLError as e:
                    errcode, errmsg = e.args
                    # 1094: Unknown thread id
                    # This means the query we were trying to kill has finished
                    # before we run kill %d
                    if errcode == 1094:
                        log.info(
                            "Trying to kill query id: {}, but it has "
                            "already finished".format(proc["Id"])
                        )
                    else:
                        raise

    def start_transaction(self):
        """
        Start a transaction. TODO: use a context manager.
        """
        self.execute_sql(sql.start_transaction)
        self.under_transaction = True

    def commit(self):
        """
        Commit and close the transaction
        """
        self.execute_sql(sql.commit)
        self.under_transaction = False

    def ddl_guard(self):
        """
        If there're already too many concurrent queries running, it's probably
        a bad idea to run DDL. Wait for some time until they finished or
        we timed out
        """
        for _ in range(self.ddl_guard_attempts):
            result = self.query(sql.show_status, ("Threads_running",))
            if result:
                threads_running = int(result[0]["Value"])
                if threads_running > self.max_running_before_ddl:
                    log.warning(
                        "Threads running: {}, bigger than allowed: {}. "
                        "Sleep 1 second before check again.".format(
                            threads_running, self.max_running_before_ddl
                        )
                    )
                    time.sleep(1)
                else:
                    log.debug(
                        "Threads running: {}, less than: {}. We are good "
                        "to go".format(threads_running, self.max_running_before_ddl)
                    )
                    return
        log.error(
            "Hit max attempts: {}, but the threads running still don't drop"
            "below: {}.".format(self.ddl_guard_attempts, self.max_running_before_ddl)
        )
        raise OSCError(OSCError.Errors.DDL_GUARD_ATTEMPTS)

    @wrap_hook
    def lock_tables(self, tables):
        for _ in range(self.lock_max_attempts):
            # We use a threading.Timer with a second connection in order to
            # kill any selects on top of the tables being altered if we could
            # not lock the tables in time
            another_conn = self.get_conn(self._current_db)
            kill_timer = Timer(
                self.lock_max_wait_before_kill_seconds,
                self.kill_selects,
                args=(tables, another_conn),
            )
            # keeping a reference to kill timer helps on tests
            self._last_kill_timer = kill_timer
            kill_timer.start()

            try:
                self.execute_sql(sql.lock_tables(tables))
                # It is best to cancel the timer as soon as possible
                kill_timer.cancel()
                log.info(
                    "Successfully lock table(s) for write: {}".format(", ".join(tables))
                )
                break
            except MySQLdb.MySQLError as e:
                errcode, errmsg = e.args
                # 1205 is timeout and 1213 is deadlock
                if errcode in (1205, 1213):
                    log.warning("Retry locking because of error: {}".format(e))
                else:
                    raise
            finally:
                # guarantee that we dont leave a stray kill timer running
                # or any open resources
                kill_timer.cancel()
                kill_timer.join()
                another_conn.close()

        else:
            # Cannot lock write after max lock attempts
            raise OSCError(
                OSCError.Errors.FAILED_TO_LOCK_TABLE, {"tables": ", ".join(tables)}
            )

    def unlock_tables(self):
        self.execute_sql(sql.unlock_tables)
        log.info("Table(s) unlocked")

    @wrap_hook
    def create_triggers(self):
        self.stop_slave_sql()
        self.ddl_guard()
        log.debug("Locking table: {} before creating trigger".format(self.table_name))
        if not self.is_high_pri_ddl_supported:
            self.wait_until_slow_query_finish()
            self.lock_tables(tables=[self.table_name])

        try:
            log.info("Creating triggers")
            # Because we've already hold the WRITE LOCK on the table, it's now safe
            # to deal with operations that require metadata lock
            self.create_insert_trigger()
            self.create_delete_trigger()
            self.create_update_trigger()
        except Exception as e:
            if not self.is_high_pri_ddl_supported:
                self.unlock_tables()
            self.start_slave_sql()
            log.error("Failed to execute sql for creating triggers")
            raise OSCError(OSCError.Errors.CREATE_TRIGGER_ERROR, {"msg": str(e)})

        if not self.is_high_pri_ddl_supported:
            self.unlock_tables()
        self.start_slave_sql()

    def disable_ttl_for_myrocks(self):
        if self.mysql_vars.get("rocksdb_enable_ttl", "OFF") == "ON":
            self.execute_sql(sql.set_global_variable("rocksdb_enable_ttl"), ("OFF",))
            self.is_ttl_disabled_by_me = True
        else:
            log.debug("TTL not enabled for MyRocks, skip")

    def enable_ttl_for_myrocks(self):
        if self.is_ttl_disabled_by_me:
            self.execute_sql(sql.set_global_variable("rocksdb_enable_ttl"), ("ON",))
        else:
            log.debug("TTL not enabled for MyRocks before schema change, skip")

    def get_table_timestamp(self) -> str:
        table_timestamp = ""
        # Capture table create timestamp. We will continue to monitor this timestamp.
        # A change to this timestamp is indicative of a DDL operation like truncate
        # table that might have been executed while OSC is in progress.
        try:
            result = self.query(sql.get_table_timestamp(self.table_name))
            table_timestamp = result[0]["LATEST_TIME"]
        except Exception:
            log.exception(
                "Error while retrieving table create timestamp for {}".format(
                    self.table_name
                )
            )
        return table_timestamp

    def extract_gtid_set_from_snapshot_query_result(self, queryResult) -> str:
        return queryResult[0]["Gtid_executed"]

    def get_current_gtid_set(self) -> str:
        return self.current_gtid_set

    @wrap_hook
    def start_snapshot(self):
        # We need to disable TTL feature in MyRocks. Otherwise rows will
        # possibly be purged during dump/load, and cause checksum mismatch
        if self.is_myrocks_table and self.is_myrocks_ttl_table:
            log.debug("It's schema change for MyRocks table which is using TTL")
            self.disable_ttl_for_myrocks()

        snapshot_with_gtid_set_query = (
            sql.start_transaction_with_rocksdb_snapshot
            if self.is_myrocks_table
            else sql.start_transaction_with_innodb_snapshot
        )
        self.current_gtid_set = self.extract_gtid_set_from_snapshot_query_result(
            self.query(snapshot_with_gtid_set_query)
        )
        log.info("Start snapshot with GTID set: {}".format(self.current_gtid_set))
        current_max = self.get_max_delta_id()
        log.info(
            "Changes with id <= {} committed before dump snapshot, "
            "and should be ignored.".format(current_max)
        )
        # Only replay changes in the range (last_replayed_id, max_id_now]
        new_changes = self.query(
            sql.get_replay_row_ids(
                self.IDCOLNAME,
                self.DMLCOLNAME,
                self.delta_table_name,
                self.new_pk_list,
                None,
                self.mysql_version.is_mysql8,
            ),
            (
                self.last_replayed_id,
                current_max,
            ),
        )
        self._replayed_chg_ids.extend([r[self.IDCOLNAME] for r in new_changes])
        self.last_replayed_id = current_max

    def affected_rows(self):
        return self._conn.conn.affected_rows()

    def refresh_range_start(self):
        self.execute_sql(sql.select_into(self.range_end_vars, self.range_start_vars))

    def select_full_table_into_outfile(self):
        stage_start_time = time.time()
        try:
            outfile = self._outfile_name(chunk_id=1)
            sql_string = sql.select_full_table_into_file(
                self._pk_for_filter + self.old_non_pk_column_list,
                self.table_name,
                self.where,
                enable_outfile_compression=self.enable_outfile_compression,
            )
            affected_rows = self.execute_sql(
                sql_string,
                (
                    self._outfile_name(
                        chunk_id=1,
                        # MySQL does create the file with the extension itself
                        skip_compressed_extension=True,
                    ),
                ),
            )
            self.outfile_suffix_start = 1
            self.outfile_suffix_end = 1
            self.stats["outfile_lines"] = affected_rows
            self.stats["outfile_size"] = (
                os.path.getsize(outfile) if not self.use_sql_wsenv else 0
            )
            self._cleanup_payload.add_file_entry(outfile)
            self.commit()
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # 1086: File exists
            if errnum == 1086:
                raise OSCError(OSCError.Errors.FILE_ALREADY_EXIST, {"file": outfile})
            else:
                raise
        self.stats["time_in_dump"] = time.time() - stage_start_time

    @wrap_hook
    def select_chunk_into_outfile(self, use_where):
        # MySQL will append compressed extension to file name if needed.
        outfile = self._outfile_name(
            chunk_id=self.outfile_suffix_end,
            skip_compressed_extension=True,
        )

        try:
            sql_string = sql.select_full_table_into_file_by_chunk(
                self.table_name,
                self.range_start_vars_array,
                self.range_end_vars_array,
                self._pk_for_filter,
                self.old_non_pk_column_list,
                self.select_chunk_size,
                use_where,
                self.where,
                self._idx_name_for_filter,
                enable_outfile_compression=self.enable_outfile_compression,
            )
            affected_rows = self.execute_sql(sql_string, (outfile,))
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # 1086: File exists
            if errnum == 1086:
                raise OSCError(OSCError.Errors.FILE_ALREADY_EXIST, {"file": outfile})
            else:
                raise

        # Now get the real outfile name with compressed extension if needed.
        outfile = self._outfile_name(chunk_id=self.outfile_suffix_end)

        log.debug("{} affected".format(affected_rows))
        self.stats["outfile_lines"] = affected_rows + self.stats.setdefault(
            "outfile_lines", 0
        )
        self.stats["outfile_cnt"] = 1 + self.stats.setdefault("outfile_cnt", 0)
        self.stats["outfile_size"] = (
            os.path.getsize(outfile) + self.stats.setdefault("outfile_size", 0)
            if not self.use_sql_wsenv
            else 0
        )
        self._cleanup_payload.add_file_entry(outfile)
        return affected_rows

    @wrap_hook
    def log_dump_progress(self, outfile_suffix):
        progress = "Dump progress: {}/{}(ETA) chunks".format(
            outfile_suffix, self.eta_chunks
        )
        self.stats["dump_progress"] = progress
        log.info(progress)

    @wrap_hook
    def dump_table_native(self):
        """
        Dump table using DUMP TABLE statement. Uses built-in parallelism.
        """
        log.info(
            "Dumping using DUMP TABLE statement "
            f"with {self.dump_threads} threads and "
            f"chunk_size = {self.chunk_size} bytes."
        )
        dump_sql = sql.dump_table_stmt(
            self.table_name,
            self.outfile,
            self.chunk_size,
            self.dump_threads,
            consistent=True,
        )
        result = self.query(dump_sql)
        if not result:
            raise OSCError(
                OSCError.Errors.OSC_INTERNAL_ERROR,
                {"msg": "Missing result from DUMP TABLE statement."},
            )

        for r in result:
            log.info(r)

        num_chunks = result[0]["num_chunks"]

        # Update the last suffix ID for loading.
        self.outfile_suffix_end = num_chunks - 1

        # Add the list of chunk file names to be cleaned up during load time.
        for i in range(num_chunks):
            self._cleanup_payload.add_file_entry(self._outfile_name(chunk_id=i))

    @stop_if_table_timestamp_changed
    @wrap_hook
    def dump_table(self):
        """
        Dumps the source table into one or more chunk files using one of several
        methods.
        """
        log.info("== Stage 2: Dump ==")
        stage_start_time = time.time()
        if self.use_dump_table_stmt:
            # Dump via DUMP TABLE statement.
            self.dump_table_native()
        else:
            # Dump via one or more SELECT INTO OUTFILE statements.
            self.select_table_into_outfile()
        log.info("Dump finished")
        self.stats["time_in_dump"] = time.time() - stage_start_time

    @wrap_hook
    def select_table_into_outfile(self):
        log.info("Dumping using SELECT INTO OUTFILE.")

        # We can not break the table into chunks when there's no existing pk
        # We'll have to use one big file for copy data
        if self.is_full_table_dump:
            log.info("Dumping full table in one go.")
            return self.select_full_table_into_outfile()
        outfile_suffix = 1
        self.outfile_suffix_start = 1
        # To let the loop run at least once
        affected_rows = 1
        use_where = False
        printed_chunk = 0
        while affected_rows:
            self.outfile_suffix_end = outfile_suffix
            affected_rows = self.select_chunk_into_outfile(use_where)
            # Refresh where condition range for next select
            if affected_rows:
                self.refresh_range_start()
                use_where = True
                outfile_suffix += 1
            self.check_disk_free_space_reserved()
            self.perform_gc_collection()
            progress_pct = int((float(outfile_suffix) / self.eta_chunks) * 100)
            progress_chunk = int(progress_pct / 10)
            if progress_chunk > printed_chunk and self.eta_chunks > 10:
                self.log_dump_progress(outfile_suffix)
                printed_chunk = progress_chunk
        self.commit()

    @stop_if_table_timestamp_changed
    @wrap_hook
    def drop_non_unique_indexes(self):
        """
        Drop non-unique indexes from the new table to speed up the load
        process
        """
        for idx in self.droppable_indexes:
            log.info("Dropping index '{}' on intermediate table".format(idx.name))
            self.ddl_guard()
            self.execute_sql(sql.drop_index(idx.name, self.new_table_name))

    @wrap_hook
    def load_chunk(self, column_list, chunk_id):
        sql_string = sql.load_data_infile(
            self.new_table_name,
            column_list,
            ignore=self.eliminate_dups,
            enable_outfile_compression=self.enable_outfile_compression,
        )
        log.debug(sql_string)
        filepath = self._outfile_name(chunk_id)
        self.load_chunk_file(filepath, sql_string, chunk_id)
        # Delete the outfile once we have the data in new table to free
        # up space as soon as possible
        if not (self.skip_chunk_cleanup or self.use_sql_wsenv) and self.rm_file(
            filepath
        ):
            util.sync_dir(self.outfile_dir)
            self._cleanup_payload.remove_file_entry(filepath)

    # chunk_id can be used for tracking by hooks etc.
    def load_chunk_file(self, filepath, sql_string: str, chunk_id: int) -> None:
        affected_rows = self.execute_sql(sql_string, (filepath,))
        log.debug(
            f"Loaded {affected_rows} rows from file {filepath} (chunk {chunk_id})"
        )

    def change_explicit_commit(self, enable=True):
        """
        Turn on/off rocksdb_commit_in_the_middle to avoid commit stall for
        large data infiles
        """
        if self.may_have_dup_unique_keys():
            log.warning("Disable explicit_commit, because there may be duplicate keys.")
            return
        log.info(
            "explicit_commit is enabled" if enable else "explicit_commit is disabled"
        )
        v = 1 if enable else 0
        try:
            self.execute_sql(
                sql.set_session_variable("rocksdb_commit_in_the_middle"), (v,)
            )
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # 1193: unknown variable
            if errnum == 1193:
                log.warning(
                    "Failed to set rocksdb_commit_in_the_middle: {}".format(errmsg)
                )
            else:
                raise

    def change_rocksdb_bulk_load(self, enable=True):
        bulk_load_params = self.get_bulk_load_parameters()
        if bulk_load_params.should_disable_bulk_load:
            return
        v = 1 if enable else 0
        log.info("Bulk load is enabled" if enable else "Bulk load is disabled")
        #  rocksdb_bulk_load and rocksdb_bulk_load_allow_sk have the
        #  following sequence requirement so setting values accordingly.
        #  SET SESSION rocksdb_bulk_load_enable_unique_key_check=1;
        #  SET SESSION rocksdb_bulk_load_allow_sk=1;
        #  SET SESSION rocksdb_bulk_load_allow_unsorted = 1;
        #  SET SESSION rocksdb_bulk_load=1;
        #  ... (bulk loading)
        #  SET SESSION rocksdb_bulk_load=0;
        #  SET SESSION rocksdb_bulk_load_allow_sk=0;
        #  SET SESSION rocksdb_bulk_load_allow_unsorted = 0;
        #  SET SESSION rocksdb_bulk_load_enable_unique_key_check=0;
        try:
            if self.rocksdb_bulk_load_allow_sk and enable:
                if bulk_load_params.use_bulk_load_with_uk_check:
                    self.execute_sql(
                        sql.set_session_variable(
                            "rocksdb_bulk_load_enable_unique_key_check"
                        ),
                        (v,),
                    )
                self.execute_sql(
                    sql.set_session_variable("rocksdb_bulk_load_allow_sk"), (v,)
                )
                if bulk_load_params.use_bulk_load_with_pk_charset:
                    self.execute_sql(
                        sql.set_session_variable("rocksdb_bulk_load_allow_unsorted"),
                        (v,),
                    )
            self.execute_sql(sql.set_session_variable("rocksdb_bulk_load"), (v,))
            if self.rocksdb_bulk_load_allow_sk and not enable:
                self.execute_sql(
                    sql.set_session_variable("rocksdb_bulk_load_allow_sk"), (v,)
                )
                if bulk_load_params.use_bulk_load_with_pk_charset:
                    self.execute_sql(
                        sql.set_session_variable("rocksdb_bulk_load_allow_unsorted"),
                        (v,),
                    )
                if bulk_load_params.use_bulk_load_with_uk_check:
                    self.execute_sql(
                        sql.set_session_variable(
                            "rocksdb_bulk_load_enable_unique_key_check"
                        ),
                        (v,),
                    )

        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # 1193: unknown variable
            if errnum == 1193:
                log.warning("Failed to set rocksdb_bulk_load: {}".format(errmsg))
            else:
                raise

    def start_rocksdb_new_bulk_load(self):
        log.info("Use new rocksdb bulk load")
        assert self.bulk_load_session_id, "bulk load session id must be provided"
        try:
            # add cleanup no matter bulk load start succeeds or not, just in case
            self._cleanup_payload.add_sql_entry(
                sql.rollback_rocksdb_new_bulk_load(self.bulk_load_session_id),
                self.current_db,
            )
            self.execute_sql(
                sql.start_rocksdb_new_bulk_load(
                    self.bulk_load_session_id, self.new_table_name
                )
            )
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            if errnum == mysql_errors.DA_BULK_LOAD:
                raise OSCError(
                    OSCError.Errors.NEW_BULK_LOAD_EXCEPTION, {"errmsg": errmsg}
                ) from e
            else:
                raise

    def finish_rocksdb_new_bulk_load(self):
        assert self.bulk_load_session_id, "bulk load session id must be provided"
        try:
            self.execute_sql(sql.end_rocksdb_new_bulk_load(self.bulk_load_session_id))
            self.execute_sql(
                sql.commit_rocksdb_new_bulk_load(self.bulk_load_session_id)
            )
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            if errnum == mysql_errors.DA_BULK_LOAD:
                raise OSCError(
                    OSCError.Errors.NEW_BULK_LOAD_EXCEPTION, {"errmsg": errmsg}
                ) from e
            else:
                raise

    def get_bulk_load_parameters(self) -> BulkLoadParams:
        # rocksdb_bulk_load relies on data being dumping in the same sequence
        # as new pk.
        # Disable bulk load only when:
        # 1. PK columns changes
        # 2. PK columns don't change
        # but PK charset/collate changes and not casting_possible
        # New bulk load supports PK column/charset/collate changes
        # and always does unique key checks
        if self.bulk_load_session_id:
            return BulkLoadParams(False, False, True)
        use_bulk_load_with_pk_charset = False
        use_bulk_load_with_uk_check = False
        pk_collate_chg = False
        if self._old_table.primary_key != self._new_table.primary_key:
            log.warning("Disable bulk load, because we are changing PK")
            return BulkLoadParams(True, False, False)
        elif self.may_have_dup_unique_keys():
            log.warning("there may be duplicated unique keys")
            use_bulk_load_with_uk_check = True
        else:
            casting_possible = False
            new_cols = {col.name: col for col in self._new_table.column_list}
            for idx, col_name in enumerate(self._pk_for_filter):
                if (
                    new_cols[col_name].charset != self._pk_for_filter_def[idx].charset
                    or new_cols[col_name].collate
                    != self._pk_for_filter_def[idx].collate
                ):
                    pk_collate_chg = True
                    if (
                        new_cols[col_name].column_type == "VARCHAR"
                        and self._pk_for_filter_def[idx].column_type == "VARBINARY"
                    ):
                        casting_possible = True

                    use_bulk_load_with_pk_charset = True
                    log.warning(
                        "we are changing PK column charset/collate",
                    )
                    break
            if pk_collate_chg and casting_possible:
                for col in self._new_table.column_list:
                    self.mismatch_pk_charset[col.name] = new_cols[col.name].charset
        log.warning(
            "use_bulk_load_with_pk_charset="
            + str(use_bulk_load_with_pk_charset)
            + ". use_bulk_load_with_uk_check="
            + str(use_bulk_load_with_uk_check)
        )
        return BulkLoadParams(
            pk_collate_chg and not casting_possible,
            use_bulk_load_with_pk_charset,
            use_bulk_load_with_uk_check,
        )

    def may_have_dup_unique_keys(self):
        diff = SchemaDiff(self._old_table, self._new_table)
        return not self.eliminate_dups and (
            IndexAlterType.BECOME_UNIQUE_INDEX in diff.alter_types
        )

    @wrap_hook
    def log_load_progress(self, suffix):
        progress = "Load progress: {}/{} chunks".format(suffix, self.outfile_suffix_end)
        self.stats["load_progress"] = progress
        log.info(progress)

    @stop_if_table_timestamp_changed
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
            raise OSCError(
                OSCError.Errors.OSC_INTERNAL_ERROR,
                {
                    "msg": "Unexpected scenario. Both _pk_for_filter "
                    "and old_non_pk_column_list are empty"
                },
            )
        if self.is_myrocks_table:
            if not self.bulk_load_session_id:
                # Enable rocksdb bulk load before loading data
                self.change_rocksdb_bulk_load(enable=True)
                # Enable rocksdb explicit commit before loading data
                self.change_explicit_commit(enable=True)
            else:
                self.start_rocksdb_new_bulk_load()

        # Print out information after every 5% chunks have been loaded
        chunk_pct_for_progress = 5
        progress_freq = int(self.outfile_suffix_end * chunk_pct_for_progress / 100.0)
        for suffix in range(self.outfile_suffix_start, self.outfile_suffix_end + 1):
            self.load_chunk(column_list, suffix)
            self.perform_gc_collection()
            # We won't show progress if the number of chunks is less than 100
            if suffix % max(5, progress_freq) == 0:
                self.log_load_progress(suffix)

        if self.is_myrocks_table:
            if not self.bulk_load_session_id:
                # Disable rocksdb bulk load after loading data
                self.change_rocksdb_bulk_load(enable=False)
                # Disable rocksdb explicit commit after loading data
                self.change_explicit_commit(enable=False)
            else:
                self.finish_rocksdb_new_bulk_load()
        self.stats["time_in_load"] = time.time() - stage_start_time

    def check_max_statement_time_exists(self):
        """
        Check whether current MySQL instance support MAX_STATEMENT_TIME
        which is only supported by WebScaleSQL
        """
        if self.mysql_version.is_mysql8:
            return self.is_var_enabled("max_execution_time")
        else:
            # the max_statement_time is count in milliseconds
            try:
                self.query(sql.select_max_statement_time)
                return True
            except Exception:
                # if any exception raised here, we'll treat it as
                # MAX_STATEMENT_TIME is not supported
                log.warning("MAX_STATEMENT_TIME doesn't support in this MySQL")
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
                from_col_list=(self.IDCOLNAME, self.DMLCOLNAME),
                enable_outfile_compression=self.enable_outfile_compression,
            )
        )

    def get_max_delta_id(self):
        """
        Get current maximum delta table ID.
        """
        result = self.query(sql.get_max_id_from(self.IDCOLNAME, self.delta_table_name))
        # If no events has been replayed, max would return a string 'None'
        # instead of a pythonic None. So we should treat 'None' as 0 here
        if result[0]["max_id"] == "None":
            return max(0, self.max_id_to_replay_upto_for_good2go)
        elif self.max_id_to_replay_upto_for_good2go != -1:
            return self.max_id_to_replay_upto_for_good2go

        return result[0]["max_id"]

    @wrap_hook
    def replay_delete_row(self, replay_sql, last_id, *ids):
        """
        Replay delete type change

        @param replay_sql:  SQL statement to replay the changes stored in chg table
        @type  replay_sql:  string
        @param ids:  values of ID column from self.delta_table_name
        @type  ids:  list
        """
        affected_row = self.execute_sql(replay_sql, ids)
        if (
            not self.eliminate_dups
            and not self.where
            and not self.skip_affected_rows_check
        ):
            if not affected_row != 0:
                log.error(f"failed to replay {ids}")
                outfile = self._outfile_name(
                    suffix=".failed_replay",
                    chunk_id=0,
                    # MySQL does create the file with the extension itself
                    skip_compressed_extension=True,
                )
                # dump bad replay changes into an outfile
                self.query(
                    sql.get_replay_tbl_in_outfile(
                        self.IDCOLNAME,
                        self.delta_table_name,
                        outfile,
                    ),
                    (
                        self.last_replayed_id,
                        self.max_id_now,
                    ),
                )
                raise OSCError(
                    OSCError.Errors.REPLAY_WRONG_AFFECTED, {"num": affected_row}
                )

    @wrap_hook
    def replay_insert_row(self, sql, last_id, *ids):
        """
        Replay insert type change

        @param sql:  SQL statement to replay the changes stored in chg table
        @type  sql:  string
        @param ids:  values of ID column from self.delta_table_name
        @type  ids:  list
        """
        affected_row = self.execute_sql(sql, ids)
        if (
            not self.eliminate_dups
            and not self.where
            and not self.skip_affected_rows_check
        ):
            if not affected_row != 0:
                raise OSCError(
                    OSCError.Errors.REPLAY_WRONG_AFFECTED, {"num": affected_row}
                )

    @wrap_hook
    def replay_update_row(self, sql, last_id, *ids):
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
            "Checking {} gap ids".format(len(self._replayed_chg_ids.missing_points()))
        )
        for chg_id in self._replayed_chg_ids.missing_points():
            row = self.query(
                sql.get_chg_row(
                    self.IDCOLNAME,
                    self.DMLCOLNAME,
                    self.delta_table_name,
                    self.new_pk_list,
                ),
                (chg_id,),
            )
            if bool(row):
                log.debug("Change {} appears now!".format(chg_id))
                delta.append(row[0])
        for row in delta:
            self._replayed_chg_ids.fill(row[self.IDCOLNAME])
        log.info(
            "{} changes before last checkpoint ready for replay".format(len(delta))
        )
        return delta

    def enable_batch_updates(self):
        return False

    def divide_changes_to_group(self, chg_rows):
        """
        Put consecutive changes with the same type into a group so that we can
        execute them in a single query to speed up replay

        @param chg_rows:  list of rows returned from _chg select query
        @type  chg_rows:  list[dict]
        """
        id_group = []
        type_now = None
        tracked_primary_keys = set()
        for idx, chg in enumerate(chg_rows):
            # Start of the current group
            if type_now is None:
                type_now = chg[self.DMLCOLNAME]
            id_group.append(chg[self.IDCOLNAME])

            # Dump when we are at the end of the changes
            if idx == len(chg_rows) - 1:
                yield type_now, id_group
                tracked_primary_keys = set()
                return

            if self.use_batch_updates:
                if (
                    idx + 1 <= len(chg_rows) - 1
                    and chg_rows[idx + 1][self.DMLCOLNAME] == self.DML_TYPE_UPDATE
                ):
                    primary_key_value = ""
                    if type_now == self.DML_TYPE_UPDATE:
                        for col in self.new_pk_list:
                            primary_key_value += str(chg[col])
                            # __osc__ is the delimiter
                            primary_key_value += ";__osc__;"
                    future_key_value = ""
                    for col in self.new_pk_list:
                        future_key_value += str(chg_rows[idx + 1][col])
                        # __osc__ is the delimiter
                        future_key_value += ";__osc__;"
                    # If we have an existing update in the tracked primary keys,
                    # end the batch right now.
                    if (
                        future_key_value in tracked_primary_keys
                        or future_key_value == primary_key_value
                    ):
                        yield type_now, id_group
                        type_now = None
                        id_group = []
                        tracked_primary_keys = set()
                        continue

            # The next change is a different type, dump what we have now
            if chg_rows[idx + 1][self.DMLCOLNAME] != type_now:
                yield type_now, id_group
                type_now = None
                id_group = []
                tracked_primary_keys = set()
            # Reach the max group size, let's submit the query for now
            elif len(id_group) >= self.replay_group_size:
                yield type_now, id_group
                type_now = None
                id_group = []
                tracked_primary_keys = set()
            # update type cannot be grouped unless these are
            # consecutive updates on different new table
            # primary keys
            elif type_now == self.DML_TYPE_UPDATE:
                primary_key_value = ""
                if self.use_batch_updates:
                    for col in self.new_pk_list:
                        primary_key_value += str(chg[col])
                        # __osc__ is the delimiter
                        primary_key_value += ";__osc__;"
                    if primary_key_value not in tracked_primary_keys:
                        tracked_primary_keys.add(primary_key_value)
                        continue
                yield type_now, id_group
                type_now = None
                id_group = []
                tracked_primary_keys = set()
            # The next element will be the same as what we are now
            else:
                continue

    def perform_gc_collection(self):
        if time.time() - self.last_gc_collected > constant.GC_COLLECT_TIME_INTERVAL:
            gc_count = gc.collect(2)
            self.last_gc_collected = time.time()
            log.debug("GC collected {} objects".format(gc_count))

    def replay_changes_internal_with_delta_table(
        self, single_trx, holding_locks, delta_id_limit, stage_start_time, replay_ms
    ) -> int:
        log.info(
            f"replay_changes with delta table {single_trx=}, {holding_locks=}, {delta_id_limit=}"
        )
        # This is the old catchup implementation, we use delta table to perform
        # catchup, with the faster catchup tool this is not needed.
        max_id_now = self.determine_replay_id(
            delta_id_limit if delta_id_limit else self.get_max_delta_id()
        )

        self.current_catchup_start_time = int(stage_start_time)
        log.debug("Timeout for replay changes: {}".format(self.replay_timeout))
        time_start = stage_start_time
        deleted, inserted, updated = 0, 0, 0

        self.record_currently_replaying_id(max_id_now)
        self.max_id_now = max_id_now

        log.info("max_id_now is %r / %r", max_id_now, self.replay_max_changes)
        if max_id_now > self.replay_max_changes:
            raise OSCError(
                OSCError.Errors.REPLAY_TOO_MANY_DELTAS,
                {"deltas": max_id_now, "max_deltas": self.replay_max_changes},
            )

        if self.detailed_mismatch_info or self.dump_after_checksum:
            # We need this information for better understanding of the checksum
            # mismatch issue
            log.info(
                "Replaying changes happened before change ID: {}".format(max_id_now)
            )
        delta = self.get_gap_changes()

        # Only replay changes in this range (last_replayed_id, max_id_now]
        new_changes = self.query(
            sql.get_replay_row_ids(
                self.IDCOLNAME,
                self.DMLCOLNAME,
                self.delta_table_name,
                self.new_pk_list,
                replay_ms,
                self.mysql_version.is_mysql8,
            ),
            (
                self.last_replayed_id,
                max_id_now,
            ),
        )
        self._replayed_chg_ids.extend([r[self.IDCOLNAME] for r in new_changes])
        delta.extend(new_changes)

        log.info("Total {} changes to replay".format(len(delta)))
        # Generate all three possible replay SQL here, so that we don't waste
        # CPU time regenerating them for each replay event
        delete_sql = sql.replay_delete_row(
            self.new_table_name,
            self.delta_table_name,
            self.IDCOLNAME,
            self._pk_for_filter,
            self.mismatch_pk_charset,
        )
        update_sql = sql.replay_update_row(
            self.old_non_pk_column_list,
            self.new_table_name,
            self.delta_table_name,
            self.eliminate_dups,
            self.IDCOLNAME,
            self._pk_for_filter,
            self.mismatch_pk_charset,
        )
        insert_sql = sql.replay_insert_row(
            self.old_column_list,
            self.new_table_name,
            self.delta_table_name,
            self.IDCOLNAME,
            self.eliminate_dups,
        )
        replayed = 0
        replayed_total = 0
        showed_pct = 0
        for chg_type, ids in self.divide_changes_to_group(delta):
            # We only care about replay time when we are holding a write lock
            if (
                holding_locks
                and not self.bypass_replay_timeout
                and time.time() - time_start > self.replay_timeout
            ):
                raise OSCError(OSCError.Errors.REPLAY_TIMEOUT)
            replayed_total += len(ids)
            # Commit transaction after every replay_batch_size number of
            # changes have been replayed
            if not single_trx and replayed > self.replay_batch_size:
                self.commit()
                self.start_transaction()
                replayed = 0
            else:
                replayed += len(ids)

            # Use corresponding SQL to replay each type of changes
            if chg_type == self.DML_TYPE_DELETE:
                self.replay_delete_row(delete_sql, ids[-1], ids)
                deleted += len(ids)
            elif chg_type == self.DML_TYPE_UPDATE:
                self.replay_update_row(update_sql, ids[-1], ids)
                updated += len(ids)
            elif chg_type == self.DML_TYPE_INSERT:
                self.replay_insert_row(insert_sql, ids[-1], ids)
                inserted += len(ids)
            else:
                # We are not supposed to reach here, unless someone explicitly
                # insert a row with unknown type into _chg table during OSC
                raise OSCError(
                    OSCError.Errors.UNKOWN_REPLAY_TYPE, {"type_value": chg_type}
                )
            # Print progress information after every 10% changes have been
            # replayed. If there're no more than 100 changes to replay then
            # there'll be no such progress information
            progress_pct = int(replayed_total / len(delta) * 100)
            if progress_pct > showed_pct:
                log.info(
                    "Replay progress: {}/{} changes".format(replayed_total, len(delta))
                )
                showed_pct += 10
        # Commit for last batch
        if not single_trx:
            self.commit()
        self.last_replayed_id = max_id_now

        log.info(
            "Replayed {} INSERT, {} DELETE, {} UPDATE".format(
                inserted, deleted, updated
            )
        )
        return inserted + deleted + updated

    def replay_changes(
        self,
        single_trx=False,
        holding_locks=False,
        delta_id_limit=None,
        until_gtid_set=None,  # catch up indefinitely
    ):
        """
        Loop through all the existing events in __osc_chg table and replay
        the change

        @param single_trx:  Replay all the changes in single transaction or
        not
        @type  single_trx:  bool
        """
        assert (
            delta_id_limit is None or until_gtid_set is None
        ), "delta_id_limit and until_gtid_set cannot be both non-empty"
        # all the changes to be replayed in this round will be stored in
        # tmp_table_include_id. Though change events may keep being generated,
        # we'll only replay till the end of temporary table
        if (
            single_trx
            and not self.bypass_replay_timeout
            and self.check_max_statement_time_exists()
        ):
            replay_ms = self.replay_timeout * 1000
        else:
            replay_ms = None

        log.info(
            f"replay_changes use_gtid_for_catchup={self.fast_catchup_tool_enabled()}"
        )
        stage_start_time = time.time()
        delta_updates_count = 0
        if not self.fast_catchup_tool_enabled():
            delta_updates_count = self.replay_changes_internal_with_delta_table(
                single_trx, holding_locks, delta_id_limit, stage_start_time, replay_ms
            )
        else:
            # Run faster catchup and wait for the catchup to complete,
            # all the transformation logic would happen in the fast catchup tool
            self.catchup_tool.async_catchup_table_to_gtid_set(until_gtid_set)
            if not until_gtid_set:
                # Write the ending signal to the catchup tool, so that it can stop
                self.catchup_tool.write_stop_catchup_job_signal()
            self.catchup_tool.wait_for_catchup_job_to_finish()

        self.perform_gc_collection()
        end_time = time.time()
        self.current_catchup_end_time = int(end_time)
        time_spent = end_time - stage_start_time
        self.stats["time_in_replay"] = (
            self.stats.setdefault("time_in_replay", 0) + time_spent
        )
        log.info("Replayed in {:.2f} Seconds".format(time_spent))
        if time_spent > 0.0:
            self.stats["last_catchup_speed"] = delta_updates_count / time_spent

    def record_currently_replaying_id(self, max_id_now: int) -> None:
        return

    def determine_replay_id(self, max_replay_id: int):
        if self.max_id_to_replay_upto_for_good2go != -1:
            if (
                not max_replay_id
                or max_replay_id > self.max_id_to_replay_upto_for_good2go
            ):
                return self.max_id_to_replay_upto_for_good2go

        return max_replay_id

    def set_innodb_tmpdir(self, innodb_tmpdir):
        try:
            self.execute_sql(
                sql.set_session_variable("innodb_tmpdir"), (innodb_tmpdir,)
            )
        except MySQLdb.OperationalError as e:
            errnum, errmsg = e.args
            # data_dir cannot always be set to innodb_tmpdir due to
            # privilege issue. Falling back to tmpdir if it happens
            # 1193: unknown variable
            # 1231: Failed to set because of privilege error
            if errnum in (1231, 1193):
                log.warning(
                    "Failed to set innodb_tmpdir, falling back to tmpdir: {}".format(
                        errmsg
                    )
                )
            else:
                raise

    @stop_if_table_timestamp_changed
    @wrap_hook
    def recreate_non_unique_indexes(self):
        """
        Re-create non-unique indexes onto the new table
        """
        # Skip replaying changes for now, if don't have to recreate index
        if not self.droppable_indexes:
            return

        self.set_innodb_tmpdir(self.outfile_dir)
        # Execute alter table only if we have index to create
        if self.droppable_indexes:
            self.ddl_guard()
            log.info(
                "Recreating indexes: {}".format(
                    ", ".join(col.name for col in self.droppable_indexes)
                )
            )
            self.execute_sql(sql.add_index(self.new_table_name, self.droppable_indexes))

    @stop_if_table_timestamp_changed
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
        self.query(sql.analyze_table(self.delta_table_name))

    def compare_checksum(
        self,
        old_table_checksum: list[dict[str, int]],
        new_table_checksum: list[dict[str, int]],
    ):
        """
        Given two list of checksum result generated by checksum_by_chunk,
        compare whether there's any difference between them

        @param old_table_checksum:  checksum from old table
        @param new_table_checksum:  checksum from new table

        Raises OSCError upon mismatch.
        """
        if len(old_table_checksum) != len(new_table_checksum):
            log.error(
                "The total number of checksum chunks mismatch " "OLD={}, NEW={}".format(
                    len(old_table_checksum), len(new_table_checksum)
                )
            )
            log.info("Running detailed checksum to get more detailed diagnostics")
            self.detailed_checksum()
        log.debug("{} checksum chunks in total".format(len(old_table_checksum)))

        checksum_xor = 0
        # Also, generate an xor of all the checksum entries for a quick sanity test
        for idx, checksum_entry in enumerate(old_table_checksum):
            for col in checksum_entry:
                if not old_table_checksum[idx][col] == new_table_checksum[idx][col]:
                    log.error(
                        "checksum/count mismatch for chunk {} "
                        "column `{}`: OLD={}, NEW={}".format(
                            idx,
                            col,
                            old_table_checksum[idx][col],
                            new_table_checksum[idx][col],
                        )
                    )
                    log.error(
                        "Number of rows for the chunk that cause the "
                        "mismatch: OLD={}, NEW={}".format(
                            old_table_checksum[idx]["cnt"],
                            new_table_checksum[idx]["cnt"],
                        )
                    )
                    log.error(
                        "Current replayed max(__OSC_ID) of chg table {}".format(
                            self.last_replayed_id
                        )
                    )
                    log.info(
                        "Running detailed checksum to get more detailed diagnostics"
                    )
                    self.detailed_checksum()
                else:
                    checksum_xor ^= old_table_checksum[idx][col]

        self.current_checksum_record = checksum_xor

    def checksum_full_table(self) -> None:
        """
        Running checksum in single query, this will be used only for tables
        which don't have primary in the old schema. See checksum_by_chunk
        for more detail
        """
        # Calculate checksum for old table
        old_checksum = self.query(
            sql.checksum_full_table(
                self.table_name, self.checksum_column_list(exclude_pk=False)
            )
        )

        # Calculate checksum for new table
        new_checksum = self.query(
            sql.checksum_full_table(
                self.new_table_name, self.checksum_column_list(exclude_pk=False)
            )
        )
        self.commit()

        # Compare checksum
        if old_checksum and new_checksum:
            self.compare_checksum(old_checksum, new_checksum)

    def checksum_full_table_native(self) -> None:
        """
        Running checksum in a single query, using CHECKSUM TABLE, which should
        be faster than using aggregation functions (like BIT_XOR) and safer
        (avoids XOR pitfalls with even number of rows).
        """
        checksums = []
        for table in [self.table_name, self.new_table_name]:
            log.info(f"Calculating checksum for {table}")
            sql_query = sql.checksum_full_table_native(
                table, self.checksum_column_list(exclude_pk=False)
            )
            checksums.append(
                # Take the first row only as only one is expected.
                self.query(sql_query)[0]
            )

        self.commit()

        checksum_old = checksums[0]["Checksum"]
        checksum_new = checksums[1]["Checksum"]

        if checksum_old != checksum_new:
            log.error(f"Checksum mismatch: OLD={checksum_old}, NEW={checksum_new}. ")
            log.info("Running detailed checksum to get more detailed diagnostics")
            self.detailed_checksum()

    def checksum_for_single_chunk(self, table_name, use_where, idx_for_checksum):
        """
        Using the same set of session variable as chunk start point and
        calculate checksum for old table/new table. If assign is provided,
        current right boundary will be passed into range_start_vars as the
        start of next chunk
        """
        return self.query(
            sql.checksum_by_chunk_with_assign(
                table_name,
                self.checksum_column_list(exclude_pk=True),
                self._pk_for_filter,
                self.range_start_vars_array,
                self.range_end_vars_array,
                self.select_chunk_size,
                use_where,
                idx_for_checksum,
            )
        )[0]

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
        log.info(
            ", ".join(self._pk_for_filter + self.checksum_column_list(exclude_pk=True))
        )
        for table_name in [self.table_name, self.new_table_name]:
            if table_name == self.new_table_name:
                # index for new schema can be any indexes that provides
                # uniqueness and covering old PK lookup
                idx_for_checksum = self.find_coverage_index()
                outfile = self._outfile_name(
                    suffix=".new",
                    chunk_id=0,
                    # MySQL does create the file with the extension itself
                    skip_compressed_extension=True,
                )
            else:
                # index for old schema should always be PK
                idx_for_checksum = "PRIMARY"
                outfile = self._outfile_name(
                    suffix=".old",
                    chunk_id=0,
                    # MySQL does create the file with the extension itself
                    skip_compressed_extension=True,
                )
            log.info("Dump offending chunk from {} into {}".format(table_name, outfile))
            self.execute_sql(
                sql.dump_current_chunk(
                    table_name,
                    self.checksum_column_list(exclude_pk=True),
                    self._pk_for_filter,
                    self.range_start_vars_array,
                    self.select_chunk_size,
                    idx_for_checksum,
                    use_where,
                    enable_outfile_compression=self.enable_outfile_compression,
                ),
                (outfile,),
            )

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
        old_idx_for_checksum = "PRIMARY"
        chunk_id = 0
        while affected_rows:
            chunk_id += 1
            old_checksum = self.checksum_for_single_chunk(
                self.table_name, use_where, old_idx_for_checksum
            )
            new_checksum = self.checksum_for_single_chunk(
                self.new_table_name, use_where, new_idx_for_checksum
            )
            affected_rows = old_checksum["_osc_chunk_cnt"]
            # Need to convert to List here because dict_values type will always
            # claim two sides as different
            if list(old_checksum.values()) != list(new_checksum.values()):
                log.info("Checksum mismatch detected for chunk {}: ".format(chunk_id))
                log.info("OLD: {}".format(str(old_checksum)))
                log.info("NEW: {}".format(str(new_checksum)))
                self.dump_current_chunk(use_where)
                raise OSCError(OSCError.Errors.CHECKSUM_MISMATCH)

            # Refresh where condition range for next select
            if affected_rows:
                self.refresh_range_start()
                use_where = True

    @wrap_hook
    def checksum_by_chunk(
        self, table_name: str, dump_after_checksum: bool = False
    ) -> list[dict[str, int]]:
        """
        Run checksum-by-chunk algorithm for the given table. This is to
        make sure there's no data corruption after load and first round of
        replay
        """
        checksum_result: list[dict[str, int]] = []
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
        while affected_rows:
            # TODO: consider using self.query_array method which calls
            # self._conn.query_array for memory efficiency. self.query uses
            # a DictCursor under the hood, which stores column names for each
            # row, which may be costly.
            checksum: list[dict[str, int], ...] = self.query(
                sql.checksum_by_chunk(
                    table_name,
                    self.checksum_column_list(exclude_pk=True),
                    self._pk_for_filter,
                    self.range_start_vars_array,
                    self.range_end_vars_array,
                    self.select_checksum_chunk_size,
                    use_where,
                    idx_for_checksum,
                )
            )

            # Dump the data onto local disk for further investigation
            # This will be very helpful when there's a reproducible checksum
            # mismatch issue
            if dump_after_checksum:
                self.execute_sql(
                    sql.dump_current_chunk(
                        table_name,
                        self.checksum_column_list(exclude_pk=True),
                        self._pk_for_filter,
                        self.range_start_vars_array,
                        self.select_checksum_chunk_size,
                        idx_for_checksum,
                        use_where,
                        enable_outfile_compression=self.enable_outfile_compression,
                    ),
                    ("{}.{}".format(outfile_prefix, str(outfile_id)),),
                )
                outfile_id += 1

            # Refresh where condition range for next select
            if checksum:
                self.refresh_range_start()
                affected_rows = checksum[0]["cnt"]
                checksum_result.append(checksum[0])
                use_where = True

                # tl;dr: Python memory management needs help.
                self.perform_gc_collection()
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
            old_column_tmp = [
                col for col in self._old_table.column_list if col.name == pri_column
            ]
            if old_column_tmp:
                old_column = old_column_tmp[0]
            new_column_tmp = [
                col for col in self._new_table.column_list if col.name == pri_column
            ]
            if new_column_tmp:
                new_column = new_column_tmp[0]
            if old_column and new_column:
                if not is_equal(old_column.collate, new_column.collate):
                    log.warning(
                        "Collation of primary key column {} has been "
                        "changed. Skip checksum ".format(old_column.name)
                    )
                    return False
        # There's no way we can run checksum by chunk if the primary key cannot
        # be covered by any index of the new schema
        if not self.validate_post_alter_pk():
            if self.skip_pk_coverage_check:
                log.warning(
                    "Skipping checksuming because there's no unique index "
                    "in new table schema can perfectly cover old primary key "
                    "combination for search".format(old_column.name)
                )
                return False
        else:
            # Though we have enough coverage for primary key doesn't
            # necessarily mean we can use it for checksum, it has to be an
            # unique index as well. Skip checksum if there's no such index
            if not self.find_coverage_index():
                log.warning(
                    "Skipping checksuming because there's no unique index "
                    "in new table schema can perfectly cover old primary key "
                    "combination for search".format(old_column.name)
                )
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
                "checksum for changes, because that's inefficient"
            )
            return False
        return True

    @stop_if_table_timestamp_changed
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

        self.use_batch_updates = self.enable_batch_updates()
        log.info("batch update catchup enabled: %d", self.use_batch_updates)

        stage_start_time = time.time()
        if self.eliminate_dups:
            log.warning("Skip checksum, because --eliminate-duplicate specified")
            return

        # Replay outside of transaction so that we won't hit max allowed
        # transaction time,
        log.info("= Stage 4.1: Catch up before generating checksum =")
        self.replay_till_good2go(checksum=False)

        log.info("= Stage 4.2: Generating checksum =")
        self.start_transaction()
        # To fill the gap between old and new table since last replay
        log.info("Replay changes to bring two tables to a comparable state")
        self.checksum_required_for_replay = True
        self.replay_changes(single_trx=True)

        if self.use_checksum_statement:
            log.info("Doing full table checksum in single pass (CHECKSUM TABLE method)")
            self.checksum_full_table_native()
        # If we don't have a PK on old schema, then we are not able to checksum
        # by chunk. We'll do a full table scan for checksum instead.
        elif self.is_full_table_dump:
            log.info("Doing full table checksum in single pass (SQL method)")
            return self.checksum_full_table()
        elif self.detailed_mismatch_info:
            # Special mode of checksum for debugging.
            log.info("Doing detailed (slower) checksum for debugging.")
            self.detailed_checksum()
        else:
            # Chunk-based checksumming using SQL queries to run column-wise
            # aggregates over batches of rows.
            log.info("Doing chunk-based checksum of old and new tables.")
            log.info("1. Checksumming data from old table")
            old_table_checksum = self.checksum_by_chunk(
                self.table_name, dump_after_checksum=self.dump_after_checksum
            )

            # We can calculate the checksum for new table outside the
            # transaction, because the data in new table is static without
            # replaying changes.
            self.commit()

            log.info("2. Checksuming data from new table")
            new_table_checksum = self.checksum_by_chunk(
                self.new_table_name, dump_after_checksum=self.dump_after_checksum
            )

            log.info("3. Comparing old and new checksums")
            self.compare_checksum(old_table_checksum, new_table_checksum)

        self.last_checksumed_id = self.last_replayed_id
        self.record_checksum()

        log.info("Checksum match between new and old table")
        self.stats["time_in_table_checksum"] = time.time() - stage_start_time

    def record_checksum(self):
        return

    @wrap_hook
    def evaluate_replay_progress(self):
        self.stats["num_replay_attempts"] += 1
        self.stats["replay_progress"] = (
            f"Replay progress: {self.stats['num_replay_attempts']}/"
            f"{self.replay_max_attempt}(MAX ATTEMPTS)"
        )

    @stop_if_table_timestamp_changed
    @wrap_hook
    def replay_till_good2go(self, checksum, final_catchup: bool = False):
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
        log.info(
            "Replay at most {} more round(s) until we can finish in {} "
            "seconds".format(self.replay_max_attempt, self.replay_timeout)
        )
        log.info(f"use_gtid_for_catchup: {self.fast_catchup_tool_enabled()}")
        self.stats["num_replay_attempts"] = 0
        # Temporarily enable slow query log for slow replay statements
        self.execute_sql(sql.set_session_variable("long_query_time"), (1,))
        for i in range(self.replay_max_attempt):
            log.info("Catchup Attempt: {}".format(i + 1))
            self.evaluate_replay_progress()
            start_time = time.time()
            # If checksum is required, then we need to make sure total time
            # spent in replay+checksum is below replay_timeout.
            if checksum and self.need_checksum():
                self.start_transaction()
                log.info(
                    "Catch up in order to compare checksum for the "
                    "rows that have been changed"
                )
                self.checksum_required_for_replay = True
                if self.fast_catchup_tool_enabled():
                    # If fast catchup tool is enabled, then we have to call
                    # the replay_changes() with a clear gtid set to know where
                    # to catchup to. In this case we want to catch up to the
                    # consistent snapshot that we have taken.
                    self.replay_changes(
                        single_trx=True,
                        until_gtid_set=self.current_gtid_set,
                    )
                else:
                    self.replay_changes(single_trx=True)
                self.checksum_for_changes(single_trx=False)
            else:
                if self.fast_catchup_tool_enabled():
                    # If we are using GTID for catchup, there is no need to break
                    # the replay into smaller chunks because the data is streamed
                    # to the destination table.
                    self.replay_changes(
                        single_trx=False,
                        until_gtid_set=self.current_gtid_set,
                    )
                else:
                    # Break replay into smaller chunks if it's too big
                    self.checksum_required_for_replay = False
                    max_id_now = self.get_max_delta_id()
                    while (
                        max_id_now - self.last_replayed_id > self.max_replay_batch_size
                    ):
                        delta_id_limit = (
                            self.last_replayed_id + self.max_replay_batch_size
                        )
                        log.info("Replay up to {}".format(delta_id_limit))
                        self.replay_changes(
                            single_trx=False,
                            delta_id_limit=delta_id_limit,
                        )
                    self.replay_changes(
                        single_trx=False,
                        delta_id_limit=max_id_now,
                    )

            time_in_replay = time.time() - start_time
            if time_in_replay < self.replay_timeout:
                log.info(
                    "Time spent in last round of replay is {:.2f}, which "
                    "is less than replay_timeout: {} for final replay. "
                    "We are good to proceed".format(time_in_replay, self.replay_timeout)
                )
                break
        else:
            # We are not able to bring the replay time down to replay_timeout
            if not self.bypass_replay_timeout:
                raise OSCError(
                    OSCError.Errors.MAX_ATTEMPT_EXCEEDED,
                    {"timeout": self.replay_timeout},
                )
            else:
                log.warning(
                    "Proceed after max replay attempts exceeded. "
                    "Because --bypass-replay-timeout is specified"
                )

    def get_max_replay_batch_size(self) -> int:
        return self.max_replay_batch_size

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
                    table_name,
                    self.delta_table_name,
                    # This query only uses PK for the join condition, so don't
                    # exclude them from the checksum itself.
                    self.checksum_column_list(exclude_pk=False),
                    self._pk_for_filter,
                    self.IDCOLNAME,
                    id_limit,
                    self.last_replayed_id,
                    self.replay_batch_size,
                )
            )
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
            log.warning("Skip checksum, because --eliminate-duplicate " "specified")
            return
        elif not self.need_checksum_for_changes():
            return
        # Because chunk checksum use old pk combination for searching row
        # If we don't have a pk/uk on old table then it'll be very slow, so we
        # have to skip here
        elif self.is_full_table_dump:
            return
        else:
            log.info(
                "Running checksum for rows have been changed since "
                "last checksum from change ID: {}".format(self.last_checksumed_id)
            )
        start_time = time.time()
        old_table_checksum = self.checksum_by_replay_chunk(self.table_name)
        # Checksum for the __new table should be issued inside the transaction
        # too. Otherwise those invisible gaps in the __chg table will show
        # up when calculating checksums
        new_table_checksum = self.checksum_by_replay_chunk(self.new_table_name)
        # After calculation checksums from both tables, we now can close the
        # transaction, if we want
        if not single_trx:
            self.commit()
        self.compare_checksum(old_table_checksum, new_table_checksum)
        self.last_checksumed_id = self.last_replayed_id
        self.stats["time_in_delta_checksum"] = self.stats.setdefault(
            "time_in_delta_checksum", 0
        ) + (time.time() - start_time)

        self.record_checksum()

    @wrap_hook
    def apply_partition_differences(
        self, parts_to_drop: Optional[Set[str]], parts_to_add: Optional[Set[str]]
    ) -> None:
        # we can just drop partitions by name (ie, p[0-9]+), but to add
        # partitions we need the range value for each - get this from orig
        # table
        if parts_to_add:
            add_parts = []
            for part_name in parts_to_add:
                part_value = self.partition_value_for_name(self.table_name, part_name)
                add_parts.append(
                    "PARTITION {} VALUES LESS THAN ({})".format(part_name, part_value)
                )
            add_parts_str = ", ".join(add_parts)
            add_sql = "ALTER TABLE `{}` ADD PARTITION ({})".format(
                self.new_table_name, add_parts_str
            )
            log.info(add_sql)
            self.execute_sql(add_sql)

        if parts_to_drop:
            drop_parts_str = ", ".join(parts_to_drop)
            drop_sql = "ALTER TABLE `{}` DROP PARTITION {}".format(
                self.new_table_name, drop_parts_str
            )
            log.info(drop_sql)
            self.execute_sql(drop_sql)

    @wrap_hook
    def partition_value_for_name(self, table_name: str, part_name: str) -> str:
        result = self.query(
            sql.fetch_partition_value,
            (
                self._current_db,
                table_name,
                part_name,
            ),
        )
        for r in result:
            return r["PARTITION_DESCRIPTION"]
        raise RuntimeError(f"No partition value found for {table_name} {part_name}")

    @wrap_hook
    def list_partition_names(self, table_name: str) -> List[str]:
        tbl_parts = []
        result = self.query(sql.fetch_partition, (self._current_db, table_name))
        for r in result:
            tbl_parts.append(r["PARTITION_NAME"])
        if not tbl_parts:
            raise RuntimeError(f"No partition values found for {table_name}")
        return tbl_parts

    @wrap_hook
    def sync_table_partitions(self) -> None:
        """
        If table partitions have changed on the original table, apply the same
        changes before swapping table, or we will likely break replication
        if using row-based.
        """
        log.info("== Stage 5.1: Check table partitions are up-to-date ==")

        # we're using partitions in the ddl file, skip syncing anything
        if not self.rm_partition:
            return
        # not a partitioned table, nothing to do
        if not self.partitions:
            return

        # only apply this logic to RANGE partitioning, as other types
        # are usually static
        partition_method = self.get_partition_method(
            self._current_db, self.new_table_name
        )
        if partition_method != "RANGE":
            return

        try:
            new_tbl_parts = self.list_partition_names(self.new_table_name)
            orig_tbl_parts = self.list_partition_names(self.table_name)

            parts_to_drop = set(new_tbl_parts) - set(orig_tbl_parts)
            parts_to_add = set(orig_tbl_parts) - set(new_tbl_parts)

            # information schema literally has the string None for
            # non-partitioned tables.  Previous checks *should* prevent us
            # from hitting this.
            if "None" in parts_to_add or "None" in parts_to_drop:
                log.warning(
                    "MySQL claims either %s or %s are not partitioned",
                    self.new_table_name,
                    self.table_name,
                )
                return

            if parts_to_drop:
                log.info(
                    "Partitions missing from source table "
                    "to drop from new table %s: %s",
                    self.new_table_name,
                    ", ".join(parts_to_drop),
                )
            if parts_to_add:
                log.info(
                    "Partitions in source table to add to new table %s: %s",
                    self.new_table_name,
                    ", ".join(parts_to_add),
                )
            self.apply_partition_differences(parts_to_drop, parts_to_add)
        except Exception:
            log.exception(
                "Unable to sync new table %s with orig table %s partitions",
                self.new_table_name,
                self.table_name,
            )

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
        self.execute_sql(sql.set_session_variable("autocommit"), (0,))
        self.start_transaction()
        stage_start_time = time.time()
        self.lock_tables((self.new_table_name, self.table_name, self.delta_table_name))
        log.info("Final round of replay before swap table")
        self.checksum_required_for_replay = False
        self.replay_changes(single_trx=True, holding_locks=True)
        # We will not run delta checksum here, because there will be an error
        # like this, if we run a nested query using `NOT EXISTS`:
        # SQL execution error: [1100] Table 't' was not locked with LOCK TABLES
        if self.mysql_version.is_mysql8:
            # mysql 8.0 supports atomic rename inside WRITE locks
            self.execute_sql(
                sql.rename_all_tables(
                    orig_name=self.table_name,
                    old_name=self.renamed_table_name,
                    new_name=self.new_table_name,
                )
            )
            self.table_swapped = True
            self.add_drop_table_entry(self.renamed_table_name)
            log.info(
                "Renamed {} TO {}, {} TO {}".format(
                    self.table_name,
                    self.renamed_table_name,
                    self.new_table_name,
                    self.table_name,
                )
            )
        else:
            self.execute_sql(sql.rename_table(self.table_name, self.renamed_table_name))
            log.info(
                "Renamed {} TO {}".format(self.table_name, self.renamed_table_name)
            )
            self.table_swapped = True
            self.add_drop_table_entry(self.renamed_table_name)
            self.execute_sql(sql.rename_table(self.new_table_name, self.table_name))
            log.info("Renamed {} TO {}".format(self.new_table_name, self.table_name))

        log.info("Table has successfully swapped, new schema takes effect now")
        self._cleanup_payload.remove_drop_table_entry(
            self._current_db, self.new_table_name
        )
        self.commit()
        self.unlock_tables()
        self.stats["time_in_lock"] = self.stats.setdefault("time_in_lock", 0) + (
            time.time() - stage_start_time
        )
        self.execute_sql(sql.set_session_variable("autocommit"), (1,))
        self.start_slave_sql()
        self.stats["swap_table_progress"] = "Swap table finishes"

    def rename_back(self):
        """
        If the original table was successfully renamed to _old but the second
        rename operation failed, rollback the first renaming
        """
        if (
            self.table_swapped
            and self.table_exists(self.renamed_table_name)
            and not self.table_exists(self.table_name)
        ):
            self.unlock_tables()
            self.execute_sql(sql.rename_table(self.renamed_table_name, self.table_name))

    @wrap_hook
    def cleanup(self):
        """
        Cleanup all the temporary thing we've created so far
        """
        log.info("== Stage 7: Cleanup ==")
        # Close current connection to free up all the temporary resource
        # and locks
        cleanup_start_time = time.time()
        try:
            self.rename_back()
            self.start_slave_sql()
            if self.is_myrocks_table and self.is_myrocks_ttl_table:
                self.enable_ttl_for_myrocks()
            self.release_osc_lock()
            self.stop_tracking_table_timestamp()
            self.close_conn()
        except Exception:
            log.exception(
                "Ignore following exception, because we want to try our "
                "best to cleanup, and free disk space:"
            )
        self._cleanup_payload.mysql_user = self.mysql_user
        self._cleanup_payload.mysql_pass = self.mysql_pass
        self._cleanup_payload.socket = self.socket
        self._cleanup_payload.get_conn_func = self.get_conn_func
        self._cleanup_payload.cleanup(self._current_db)
        # clean the gaps in the range chain because we might be in a loop.
        self._replayed_chg_ids = util.RangeChain()
        self.last_replayed_id = 0
        self.last_checksumed_id = 0
        self.current_checksum_record = -1
        self.stats["time_in_cleanup"] = time.time() - cleanup_start_time

    def print_stats(self):
        log.info("Time in dump: {:.3f}s".format(self.stats.get("time_in_dump", 0)))
        log.info("Time in load: {:.3f}s".format(self.stats.get("time_in_load", 0)))
        log.info("Time in replay: {:.3f}s".format(self.stats.get("time_in_replay", 0)))
        log.info(
            "Time in table checksum: {:.3f}s".format(
                self.stats.get("time_in_table_checksum", 0)
            )
        )
        log.info(
            "Time in delta checksum: {:.3f}s".format(
                self.stats.get("time_in_delta_checksum", 0)
            )
        )
        log.info(
            "Time in cleanup: {:.3f}s".format(self.stats.get("time_in_cleanup", 0))
        )
        log.info(
            "Time holding locks: {:.3f}s".format(self.stats.get("time_in_lock", 0))
        )
        log.info(f"Outfile count: {self.stats.get('outfile_cnt', 0)}")
        log.info(f"Outfile total rows: {self.stats.get('outfile_lines', 0)}")
        if not self.use_sql_wsenv:
            log.info(f"Outfile total size: {self.stats.get('outfile_size', 0)} bytes")

    # This method is overridden by fb_copy::fast_catchup_tool_enabled() in case of
    # running CopyV2. For CopyV1, it is turning off by default.
    def fast_catchup_tool_enabled(self) -> bool:
        return False

    @stop_if_table_timestamp_changed
    def execute_steps_to_cutover(self):
        self.sync_table_partitions()
        self.swap_tables()
        self.reset_no_pk_creation()

    @wrap_hook
    def run_ddl(self, db, sql):
        try:
            time_started = time.time()
            self._new_table = self.parse_function(sql, self.use_ast_parser)
            self._cleanup_payload.set_current_table(self.table_name)
            self._current_db = db
            self._current_db_dir = util.dirname_for_db(db)
            self.init_connection(db)
            self.init_table_obj()
            self.determine_outfile_dir()
            if self.force_cleanup:
                self.cleanup_with_force()
            if self.has_desired_schema():
                self.release_osc_lock()
                return
            self.unblock_no_pk_creation()
            self.pre_osc_check()
            self.create_delta_table()
            self.create_copy_table()
            self.create_triggers()
            self.record_table_timestamp()
            self.start_snapshot()
            self.dump_table()
            self.drop_non_unique_indexes()
            self.load_data()
            self.recreate_non_unique_indexes()
            self.analyze_table()
            self.checksum()
            log.info("== Stage 5: Catch up to reduce time for holding lock ==")
            self.replay_till_good2go(
                checksum=self.skip_delta_checksum, final_catchup=True
            )
            self.execute_steps_to_cutover()
            self.cleanup()
            self.print_stats()
            self.stats["wall_time"] = time.time() - time_started
        except (
            MySQLdb.OperationalError,
            MySQLdb.ProgrammingError,
            MySQLdb.IntegrityError,
        ) as e:
            errnum, errmsg = e.args
            log.error(
                "SQL execution error: [{}] {}\n"
                "When executing: {}\n"
                "With args: {}".format(
                    errnum, errmsg, self._sql_now, self._sql_args_now
                )
            )
            # 2013 stands for lost connection to MySQL
            # 2006 stands for MySQL has gone away
            # Both means we have been killed
            if errnum in (2006, 2013) and self.skip_cleanup_after_kill:
                # We can skip dropping table, and removing files.
                # However leaving trigger around may break
                # replication which is really bad. So trigger is the only
                # thing we need to clean up in this case
                self._cleanup_payload.remove_drop_table_entry(
                    self._current_db, self.new_table_name
                )
                self._cleanup_payload.remove_drop_table_entry(
                    self._current_db, self.delta_table_name
                )
                self._cleanup_payload.remove_all_file_entries()
            if not self.keep_tmp_table:
                self.cleanup()
            raise OSCError(
                OSCError.Errors.GENERIC_MYSQL_ERROR,
                {
                    "stage": "running DDL on db '{}'".format(db),
                    "errnum": errnum,
                    "errmsg": errmsg,
                },
                mysql_err_code=errnum,
            )
        except Exception as e:
            log.exception(
                "{0} Exception raised, start to cleanup before exit {0}".format(
                    "-" * 10
                )
            )
            # We want keep the temporary table for further investigation
            if not self.keep_tmp_table:
                self.cleanup()
            if not isinstance(e, OSCError):
                # It's a python exception
                raise OSCError(OSCError.Errors.OSC_INTERNAL_ERROR, {"msg": str(e)})
            else:
                raise
