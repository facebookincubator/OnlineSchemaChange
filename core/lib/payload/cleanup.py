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

import logging
import os

from .base import Payload
from .. import constant
from .. import util
from .. import sql
from ..sql import escape
from ..error import OSCError

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
        self._current_db = kwargs.get('db')
        self._current_table = kwargs.get('table')
        self.databases = kwargs.get('database')
        self.kill_first = kwargs.get('kill', False)
        self.kill_only = kwargs.get('kill_only', False)

    def cleanup(self, db='mysql'):
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
        self.gen_drop_sqls()
        if not self._conn:
            self._conn = self.get_conn(db)
        self.set_no_binlog()
        self.execute_sql('USE `{}`'.format(escape(db)))
        current_db = db
        for stmt, stmt_db in self.sqls_to_execute:
            try:
                # Switch to the database we are going to work on to avoid
                # cross db SQL execution
                if stmt_db != current_db:
                    self.execute_sql('USE `{}`'.format(escape(stmt_db)))
                    current_db = stmt_db
                log.info("Executing db: {} sql: {}".format(stmt_db, stmt))
                self.execute_sql(stmt)
            except Exception as e:
                log.error("Failed to execute sql for cleanup")
                raise OSCError(
                    'CLEANUP_EXECUTION_ERROR',
                    {'sql': stmt, 'msg': str(e)})
            finally:
                # cleanup all the queries no matter they are executed or not
                # to prevent them from being executed again in the next run
                self.sqls_to_execute = []

    def add_file_entry(self, filepath):
        log.debug("Cleanup file entry added: {}".format(filepath))
        self.files_to_clean.append(filepath)

    def remove_file_entry(self, filepath):
        log.debug("Cleanup file entry removed: {}".format(filepath))
        self.files_to_clean.remove(filepath)

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
            if entry['type'] == 'trigger':
                db = entry['db']
                trigger_name = entry['name']
                sql = "DROP TRIGGER IF EXISTS `{}`".format(
                    escape(trigger_name))
                self.sqls_to_execute.append((sql, db))

        log.info("Generating drop table queries")
        for entry in self.to_drop:
            if entry['type'] == 'table':
                db = entry['db']
                table = entry['name']
                # MySQL doesn't allow remove all the partitions in a
                # partitioned table, so we will leave single partition there
                # before drop the table
                if entry['partitions']:
                    entry['partitions'].pop()
                    # Gradually drop partitions, so that we will not hold
                    # metadata lock for too long and block requests with
                    # single drop table
                    for partition_name in entry['partitions']:
                        sql = (
                            "ALTER TABLE `{}` "
                            "DROP PARTITION `{}`"
                        ).format(
                            escape(table), escape(partition_name))
                        self.sqls_to_execute.append((sql, db))
                sql = "DROP TABLE IF EXISTS `{}`".format(table)
                self.sqls_to_execute.append((sql, db))
        self.to_drop = []

    def add_drop_table_entry(self, db, table, partitions=None):
        self.to_drop.append({
            'type': 'table',
            'db': db,
            'name': table,
            'partitions': partitions})

    def remove_drop_table_entry(self, db, table_name):
        for entry in self.to_drop:
            if entry['type'] == 'table' and entry['name'] == table_name:
                self.to_drop.remove(entry)

    def add_drop_trigger_entry(self, db, trigger_name):
        self.to_drop.append({
            'type': 'trigger',
            'db': db,
            'name': trigger_name})

    def run_ddl(self):
        """
        Try to search all the garbadge left over by OSC and clean them
        """
        self.cleanup()

    def search_for_tables(self):
        """
        List all the tables that may left over by OSC in last run
        """
        if self.databases:
            for db in self.databases:
                results = self.query(
                    sql.get_all_osc_tables(db),
                    (constant.PREFIX, constant.PREFIX, db,))
                for row in results:
                    self.add_drop_table_entry(
                        db, row['table_name'])
        else:
                results = self.query(
                    sql.get_all_osc_tables(),
                    (constant.PREFIX, constant.PREFIX, ))
                for row in results:
                    self.add_drop_table_entry(
                        row['db'], row['table_name'])

    def search_for_triggers(self):
        """
        List all the triggers that may left over by OSC in last run
        """
        if self.databases:
            for db in self.databases:
                results = self.query(
                    sql.get_all_osc_triggers(db),
                    (constant.PREFIX, constant.PREFIX, db,))
                for row in results:
                    self.add_drop_trigger_entry(
                        db, row['trigger_name'])
        else:
                results = self.query(
                    sql.get_all_osc_triggers(),
                    (constant.PREFIX, constant.PREFIX, ))
                for row in results:
                    self.add_drop_trigger_entry(
                        row['db'], row['trigger_name'])

    def kill_osc(self):
        """
        Kill the running OSC process if there's one running.
        """
        result = self.query(
            "SELECT IS_USED_LOCK(%s) as owner_id", (constant.OSC_LOCK_NAME,))
        owner_id = result[0]['owner_id']
        if owner_id:
            log.info("Named lock: {} is held by {}. Killing it to free up "
                     "the lock"
                     .format(constant.OSC_LOCK_NAME, owner_id))
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

        if self.kill_only:
            return

        # Cleanup triggers first, otherwise DML against orignal table may fail
        # with a "table not exist" error. Because the table which is referenced
        # in the trigger was dropped first.
        self.search_for_triggers()
        self.search_for_tables()

        # cleanup is a critical part, We need to make sure there's no other
        # OSC running
        self.get_osc_lock()
        self.cleanup()
        self.release_osc_lock()
