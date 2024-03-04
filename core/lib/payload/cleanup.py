#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import logging
import os
import re
import time

import MySQLdb

from .. import constant, sql, util
from ..error import OSCError
from ..sql import escape
from .base import Payload

log = logging.getLogger(__name__)


class CleanupPayload(Payload):
    """
    This payload is not a schema change payload itself. It'll cleanup all the
    mess left behind by last OSC run
    """

    def __init__(self, *args, **kwargs):
        super(CleanupPayload, self).__init__(*args, **kwargs)
        self.files_to_clean = []
        self.to_drop = []
        self.sqls_to_execute = []
        self._current_db = kwargs.get("db")
        self._current_table = kwargs.get("table")
        self.databases = kwargs.get("database")
        self.kill_first = kwargs.get("kill", False)
        self.kill_only = kwargs.get("kill_only", False)
        self.additional_osc_tables = kwargs.get("additional_tables", [])
        self.disable_replication = kwargs.get("disable_replication", True)
        self.print_tables = kwargs.get("print_tables", False)
        self.tables_to_print = []

    def set_current_table(self, table_name):
        self._current_table = table_name

    def cleanup(self, db="mysql"):
        """
        The actual cleanup logic, we will:
            - remove all the given files
            - drop all the given triggers
            - drop all the tables
        """
        # Remove file first, because drop trigger may fail because of a full
        # disk.
        for filepath in self.files_to_clean:
            try:
                if os.path.isfile(filepath):
                    util.rm(filepath, self.sudo)
            except Exception:
                # We will try our best to do the cleanup even when there's an
                # exception, because each cleanup entry is independent on each
                # other
                log.exception("Failed to cleanup file: {}".format(filepath))

        # Drop table and triggers
        # If we have multiple databases, re-require the connection
        # since the previous connection might already reach wait_timeout
        if not self._conn or (self.databases and len(self.databases) > 1):
            self._conn = self.get_conn(db)
        if self.print_tables:
            self.tables_to_print.append(
                ("SELECT * FROM `{}`".format(self._current_table), db)
            )
            self.print_osc_tables(db)
        self.gen_drop_sqls()
        self.get_mysql_settings()
        self.init_mysql_version()
        if self.disable_replication:
            self.set_no_binlog()
        self.stop_slave_sql()

        # Stop sql thread to avoid MDL lock contention and blocking reads before
        # running DDLs. Will use high_pri_ddl instead if it's supported
        if self.is_high_pri_ddl_supported:
            self.enable_priority_ddl()
        else:
            self.lock_tables(tables=[self.table_name])

        self.execute_sql("USE `{}`".format(escape(db)))
        current_db = db
        for stmt, stmt_db in self.sqls_to_execute:
            cleanupError = False
            try:
                # Switch to the database we are going to work on to avoid
                # cross db SQL execution
                if stmt_db != current_db:
                    self.execute_sql("USE `{}`".format(escape(stmt_db)))
                    current_db = stmt_db
                log.info("Executing on DB: [{}] sql: [{}]".format(stmt_db, stmt))
                self.execute_sql(stmt)
            except MySQLdb.OperationalError as e:
                errnum, _ = e.args
                # 1507 means the partition doesn't exist, which
                #     is most likely competing partition maintenance
                # 1508 means we tried to drop the last partition in a table
                if errnum in [1507, 1508]:
                    continue
                cleanupError = True
                error = e
            except Exception as e:
                cleanupError = True
                error = e
            if cleanupError:
                self.sqls_to_execute = []
                if not self.is_high_pri_ddl_supported:
                    self.unlock_tables()
                self.start_slave_sql()
                log.error("Failed to execute sql for cleanup")
                raise OSCError(
                    "CLEANUP_EXECUTION_ERROR", {"sql": stmt, "msg": str(error)}
                )

        if not self.is_high_pri_ddl_supported:
            self.unlock_tables()
        self.sqls_to_execute = []
        self.start_slave_sql()

    def print_osc_tables(self, db="mysql"):
        # print all tables in OSC job in test
        if not self._conn or (self.databases and len(self.databases) > 1):
            self._conn = self.get_conn(db)
        self.execute_sql("USE `{}`".format(escape(db)))
        for stmt, stmt_db in self.tables_to_print:
            # Work on the currernt db only
            if stmt_db != db:
                continue
            try:
                rows = self.query(stmt)
                for row in rows:
                    log.debug(row)
            except Exception:
                # If there's an exception (e.g. the table is renamed), just skip it
                continue

    def add_file_entry(self, filepath):
        log.debug("Cleanup file entry added: {}".format(filepath))
        self.files_to_clean.append(filepath)

    def remove_file_entry(self, filepath):
        log.debug("Cleanup file entry removed: {}".format(filepath))
        self.files_to_clean.remove(filepath)

    def remove_all_file_entries(self):
        log.debug("Removing all cleanup file entries")
        self.files_to_clean = []

    def add_sql_entry(self, sql):
        log.debug("Cleanup SQL entry added: {}".format(sql))
        self.sqls_to_execute.append(sql)

    def gen_drop_sqls(self):
        # always drop trigger first, otherwise there's a small window
        # in which we have trigger exists but not having the corresponding
        # _chg table. If a change happens during this window, then replication
        # will break
        log.info("Generating drop trigger queries")
        for entry in self.to_drop:
            if entry["type"] == "trigger":
                db = entry["db"]
                trigger_name = entry["name"]
                sql_query = "DROP TRIGGER IF EXISTS `{}`".format(escape(trigger_name))
                self.sqls_to_execute.append((sql_query, db))

        log.info("Generating drop table queries")
        for entry in self.to_drop:
            if entry["type"] == "table":
                db = entry["db"]
                table = entry["name"]

                partition_method = self.get_partition_method(db, table)
                if partition_method in ("RANGE", "LIST"):
                    # MySQL doesn't allow remove all the partitions in a
                    # partitioned table, so we will leave single partition
                    # there before drop the table
                    if entry["partitions"]:
                        entry["partitions"].pop()

                        # Gradually drop partitions, so that we will not hold
                        # metadata lock for too long and block requests with
                        # single drop table
                        log.debug(
                            "{}/{} using {} partitioning method".format(
                                db, table, partition_method
                            )
                        )
                        for partition_name in entry["partitions"]:
                            # As of version 8.0.17, MySQL does not support
                            # "DROP PARTITION IF EXISTS".
                            sql_query = (
                                "ALTER TABLE `{}` " "DROP PARTITION `{}`"
                            ).format(escape(table), escape(partition_name))
                            self.sqls_to_execute.append((sql_query, db))

                sql_query = "DROP TABLE IF EXISTS `{}`".format(table)
                self.sqls_to_execute.append((sql_query, db))

        self.to_drop = []

    def add_drop_table_entry(self, db, table, partitions=None):
        self.to_drop.append(
            {"type": "table", "db": db, "name": table, "partitions": partitions}
        )
        self.tables_to_print.append(("SELECT * FROM `{}`".format(table), db))

    def remove_drop_table_entry(self, db, table_name):
        for entry in self.to_drop:
            if entry["type"] == "table" and entry["name"] == table_name:
                self.to_drop.remove(entry)

    def add_drop_trigger_entry(self, db, trigger_name):
        self.to_drop.append({"type": "trigger", "db": db, "name": trigger_name})

    def run_ddl(self):
        """
        Try to search all the garbage left over by OSC and clean them
        """
        self.cleanup()

    def fetch_all_tables(self):
        results = self.query(
            sql.get_all_osc_tables(),
            (
                constant.PREFIX,
                constant.PREFIX,
            ),
        )
        return [row["TABLE_NAME"] for row in results]

    def search_for_tables(self):
        """
        List all the tables that may left over by OSC in last run
        """
        if self.databases:
            for db in self.databases:
                results = self.query(
                    sql.get_all_osc_tables(db),
                    (
                        constant.PREFIX,
                        constant.PREFIX,
                        db,
                    ),
                )
                for row in results:
                    self.add_drop_table_entry(db, row["TABLE_NAME"])
        else:
            results = self.query(
                sql.get_all_osc_tables(),
                (
                    constant.PREFIX,
                    constant.PREFIX,
                ),
            )
            for row in results:
                self.add_drop_table_entry(row["db"], row["TABLE_NAME"])

        for table in self.additional_osc_tables:
            log.info(f"reading table: {table}")
            results = self.query(
                sql.get_all_osc_tables(),
                (
                    table,
                    table,
                ),
            )
            for row in results:
                self.add_drop_table_entry(row["db"], row["TABLE_NAME"])

    def search_for_triggers(self):
        """
        List all the triggers that may left over by OSC in last run
        """
        if self.databases:
            for db in self.databases:
                results = self.query(
                    sql.get_all_osc_triggers(db),
                    (
                        constant.PREFIX,
                        constant.PREFIX,
                        db,
                    ),
                )
                for row in results:
                    self.add_drop_trigger_entry(db, row["TRIGGER_NAME"])
        else:
            results = self.query(
                sql.get_all_osc_triggers(),
                (
                    constant.PREFIX,
                    constant.PREFIX,
                ),
            )
            for row in results:
                self.add_drop_trigger_entry(row["db"], row["TRIGGER_NAME"])

    def search_for_files(self):
        """
        List all the files that may have been left over by OSC in previous runs

        TODO: cleaning up is also done a lot in copy.py, so a future
        improvement here could be to refactor OSC in such a way that the
        cleanup part can be easily reused. T28154647
        """
        datadir = self.query(sql.select_as("@@datadir", "dir"))[0]["dir"]
        for root, _, files in os.walk(datadir):
            for fname in files:
                if re.match(r"__osc_.*\.[0-9]+", fname):
                    self.add_file_entry(os.path.join(root, fname))

    def kill_osc(self):
        """
        Kill the running OSC process if there's one running.
        """
        result = self.query(
            "SELECT IS_USED_LOCK(%s) as owner_id", (constant.OSC_LOCK_NAME,)
        )
        owner_id = result[0]["owner_id"]
        if owner_id:
            log.info(
                "Named lock: {} is held by {}. Killing it to free up "
                "the lock".format(constant.OSC_LOCK_NAME, owner_id)
            )
            # If we kill the mysql connection which is holding the named lock,
            # then OSC's python process will encounter a "MySQL has gone away"
            # error, and do the cleanup, then exit
            self.execute_sql(sql.kill_proc, (owner_id,))
        else:
            log.info("No other OSC is running at the moment")

    def cleanup_all(self):
        """
        Try to list all the possible files/tables left over by an unclean OSC
        exit, and remove all of them
        """
        if self.kill_first:
            self.kill_osc()
            log.info("Wait 5 seconds for the running OSC to cleanup its own stuff ")
            time.sleep(5)

        if self.kill_only:
            return

        # Cleanup triggers first, otherwise DML against original table may fail
        # with a "table not exist" error. Because the table which is referenced
        # in the trigger was dropped first.
        self.search_for_triggers()
        self.search_for_tables()
        self.search_for_files()

        # cleanup is a critical part, We need to make sure there's no other
        # OSC running
        self.get_osc_lock()
        self.cleanup()
        self.release_osc_lock()
