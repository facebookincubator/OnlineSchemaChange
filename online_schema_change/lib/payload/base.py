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


import codecs
import collections
import logging
import MySQLdb

from .. import db as db_lib
from .. import constant
from .. import hook
from .. import sql
from .. import util
from ..error import OSCError
from ..sqlparse import parse_create, ParseError

log = logging.getLogger(__name__)


class Payload(object):
    """
    Base class for all supported schema change
    """

    def __init__(self, **kwargs):
        self.outfile_dir = ''
        self.repl_status = ''
        self._mysql_vars = {}
        self.session_timeout = 10
        self.sql_list = []
        self.force = False
        self.standardize = False
        self.dry_run = False
        self.mysql_engine = ''
        self._conn = None
        self._sql_now = None
        self._sql_args_now = None
        self.ddl_file_list = kwargs.get('ddl_file_list', None)
        self.get_conn_func = kwargs.get('get_conn_func', None)
        self.hook_map = kwargs.get(
            'hook_map', collections.defaultdict(lambda: hook.NoopHook()))
        self.socket = kwargs.get('socket', '')
        self.mysql_user = kwargs.get('mysql_user', '')
        self.mysql_pass = kwargs.get('mysql_password', '')
        self.charset = kwargs.get('charset', None)
        self.db_list = kwargs.get('database', [])
        self.mysql_engine = kwargs.get('mysql_engine', None)
        self.sudo = kwargs.get('sudo', False)
        self.skip_named_lock = kwargs.get(
            'skip_named_lock', False)

    @property
    def conn(self):
        """
        Access to database connection handler, which is a private var
        We do not expect reconnecting or override the connection during the
        operation. However we do support share connection handler between
        payload and hook.
        """
        return self._conn

    def init_conn(self, dbname=''):
        """
        Initialize database connection handler
        """
        if not self._conn:
            self._conn = self.get_conn(dbname)
            if self._conn:
                return True
        else:
            return True

    def get_conn(self, dbname=''):
        """
        Create the connection to MySQL instance, there will be only one
        connection during the whole schema change
        """
        try:
            conn = db_lib.MySQLSocketConnection(
                self.mysql_user, self.mysql_pass, self.socket, dbname,
                connect_function=self.get_conn_func, charset=self.charset)
            if conn:
                conn.connect()
                if self.session_timeout:
                    conn.execute("SET SESSION wait_timeout = {}"
                                 .format(self.session_timeout))
                return conn
        except MySQLdb.MySQLError as e:
            errcode, errmsg = e.args
            log.error("Error when connecting to MySQL [{}] {}"
                      .format(errcode, errmsg))
            raise OSCError(
                'GENERIC_MYSQL_ERROR',
                {'stage': 'Connecting to MySQL',
                 'errnum': errcode, 'errmsg': errmsg})

    def close_conn(self):
        """
        Close the connection after all schema changes have finished
        """
        try:
            if self._conn:
                self._conn.disconnect()
                self._conn = None
                return True
        except Exception:
            log.error("Failed to close MySQL connection to local instance")
            raise

    def use_db(self, db):
        """
        Switch db
        """
        try:
            self._conn.use(db)
        except Exception:
            log.error("Failed to change database using `use {}`".format(db))
            raise

    def check_replication_type(self):
        """
        Get current replication role for the instance attached to this payload
        """
        repl_status_now = 'slave'
        log.debug("Checking replication role type, expecting: {}"
                  .format(self.repl_status))
        r = self.query("SHOW SLAVE STATUS")
        if not r:
            repl_status_now = 'master'
        log.debug("Replication mode for database is: {}"
                  .format(repl_status_now))
        return repl_status_now == self.repl_status

    def query(self, sql, args=None):
        """
        Execute sql again MySQL instance and return the result
        """
        self._sql_now = sql
        self._sql_args_now = args
        log.debug("Running the following SQL on MySQL: {} {}"
                  .format(sql, args))
        return self._conn.query(sql, args)

    def execute_sql(self, sql, args=None):
        """
        Execute the given sql against MySQL without caring about the result
        output
        """
        self._sql_now = sql
        self._sql_args_now = args
        log.debug("Running the following SQL on MySQL: {} {}"
                  .format(sql, args))
        return self._conn.execute(sql, args)

    def fetch_mysql_vars(self):
        """
        Populate all current MySQL variables(settings) into class property
        """
        log.debug("Fetching variables from MySQL")
        variables = self._conn.query("SHOW VARIABLES")
        self._mysql_vars = {
            r['Variable_name']: r['Value'] for r in variables}
        if self._mysql_vars:
            return True

    @property
    def mysql_var(self):
        if not self._mysql_vars:
            log.exception("fetch_mysql_vars hasn't been not called before "
                          "accessing _mysql_vars")
            return []
        return self._mysql_vars

    def check_db_existence(self):
        """
        Check whether all the databases specified exist on instance attached
        to this payload
        """
        non_exist_dbs = []
        try:
            databases = self.query("SHOW DATABASES")
            dbs = {r['Database'] for r in databases}
            for db in self.db_list:
                if db not in dbs:
                    log.debug("DB: {} doesn't exist in MySQL"
                              .format(db))
                    non_exist_dbs.append(db)
            return non_exist_dbs
        except Exception:
            log.exception("Failed to check database existance")
            return False

    def read_ddl_files(self):
        """
        Read all content from the given file list, and standardize it if
        necessary
        """
        for ddl_file in self.ddl_file_list:
            with codecs.open(ddl_file, "r", "utf-8") as fh:
                raw_sql = "\n".join([
                    line for line in fh.readlines()
                    if not line.startswith('--')])
                try:
                    parsed_sql = parse_create(raw_sql)
                except ParseError as e:
                    raise OSCError(
                        'INVALID_SYNTAX',
                        {'filepath': ddl_file, 'msg': str(e)})
                # If engine enforcement is given on CLI, we need to compare
                # whether the engine in file is the same as what we expect
                if self.mysql_engine:
                    if not parsed_sql.engine:
                        log.warning(
                            "Engine enforcement specified, but engine option"
                            "is not specified in: '{}'. It will use MySQL's "
                            "default engine"
                            .format(ddl_file))
                    elif self.mysql_engine.lower() != \
                            parsed_sql.engine.lower():
                        raise OSCError('WRONG_ENGINE',
                                       {'engine': parsed_sql.engine,
                                        'expect': self.mysql_engine})
                self.sql_list.append({
                    'filepath': ddl_file,
                    'raw_sql': raw_sql,
                    'sql_obj': parsed_sql})

    def set_no_binlog(self):
        """
        Set session sql_log_bin=OFF
        """
        try:
            self._conn.set_no_binlog()
        except MySQLdb.MySQLError as e:
            errcode, errmsg = e.args
            raise OSCError(
                'GENERIC_MYSQL_ERROR',
                {'stage': 'before running ddl',
                 'errnum': errcode, 'errmsg': errmsg})

    def rm_file(self, filename):
        """Wrapper of the util.rm function. This is here mainly to make it
        eaiser for implementing a hook around the rm call

        @param filename:  Full path of the file needs to be removed
        @type  filename:  string
        """
        util.rm(filename, sudo=self.sudo)

    def get_osc_lock(self):
        """
        Grab a MySQL lock before we start OSC. This will prevent multiple
        OSC process running at the same time for single MySQL instance.
        Notice that the lock here is different from the ones in lock_tables.
        It is basically an exclusive meta lock instead of table locks
        """
        if self.skip_named_lock:
            log.warning("Skipping attempt to get lock, "
                        "because skip_named_lock is specified")
            return
        result = self.query(sql.get_lock, (constant.OSC_LOCK_NAME,))
        if not result or not result[0]['lockstatus'] == 1:
            raise OSCError("UNABLE_TO_GET_LOCK")

    def release_osc_lock(self):
        """
        Release the lock we've grabbed in self.get_osc_lock.
        Notice that the lock here is different from the ones in unlock_tables.
        It is basically an exclusive meta lock instead of table locks
        """
        if self.skip_named_lock:
            return
        result = self.query(sql.release_lock, (constant.OSC_LOCK_NAME,))
        if not result or not result[0]['lockstatus'] == 1:
            log.warning("Unable to release osc lock: {}"
                        .format(constant.OSC_LOCK_NAME))

    def run(self):
        """
        Main logic of the payload
        """
        log.info("reading SQL files")
        self.read_ddl_files()

        # Get the connection to MySQL ready, so we don't have to create a new
        # connection each time we want to execute a SQL
        if not self.init_conn():
            raise OSCError('FAILED_TO_CONNECT_DB',
                           {'user': self.mysql_user,
                            'socket': self.socket})
        self.set_no_binlog()

        # Check database existence
        if not bool(self.db_list):
            raise OSCError('DB_NOT_GIVEN')

        # Check database existence
        non_exist_dbs = self.check_db_existence()
        if non_exist_dbs:
            raise OSCError('DB_NOT_EXIST',
                           {'db_list': ', '.join(non_exist_dbs)})

        # Test whether the replication role matches
        if self.repl_status:
            if not self.check_replication_type():
                raise OSCError('REPL_ROLE_MISMATCH',
                               {'given_role': self.repl_status})

        # Fetch mysql variables from server
        if not self.fetch_mysql_vars():
            raise OSCError('FAILED_TO_FETCH_MYSQL_VARS')

        # Iterate through all the specified databases
        for db in self.db_list:
            log.info("Running changes for database: '{}'".format(db))
            # Iterate through all the given sql files
            for job in self.sql_list:
                log.info("Running SQLs from file: '{}'"
                         .format(job['filepath']))
                try:
                    if not self.init_conn():
                        raise OSCError('FAILED_TO_CONNECT_DB',
                                       {'user': self.mysql_user,
                                        'socket': self.socket})
                    if self.standardize:
                        self.run_ddl(db, job['sql_obj'].to_sql())
                    else:
                        self.run_ddl(db, job['raw_sql'])
                    log.info("Successfully run changes from file: '{}'"
                             .format(job['filepath']))
                except Exception as e:
                    if not self.force:
                        raise
                    else:
                        log.warning("Following error is ignored because of "
                                    "force mode is enabled: ")
                        log.warning("\t{}".format(e))
            log.info("Changes for database '{}' finished".format(db))

    def execute_hook(self, hook_point=''):
        """Look up predefined hook in hook_map and execute it

        @param hook_point:  Name of a hook to execute. A hook point is defined
            using ..hook.wrap_hook decorator. For example:
                @wrap_hook
                def function_foo(self):
                    pass
            will have two hook points called: 'before_function_foo' and
            'after_function_foo'
        @type  hook_point:  string
        """
        log.debug("Trigger hook point: {}".format(hook_point))
        hook_obj = self.hook_map[hook_point]
        if not isinstance(hook_obj, hook.NoopHook):
            log.debug("Executing hook: {} for hook point: {}"
                      .format(hook_obj.__class__.__name__, hook_point))
            hook_obj.execute(self)
