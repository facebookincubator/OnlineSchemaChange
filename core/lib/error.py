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


class OSCError(Exception):
    ERR_MAPPING = {
        'NON_ROOT_USER': {
            'code': 100,
            'desc': 'Non-root user execution',
        },
        'OUTFILE_DIR_NOT_EXIST': {
            'code': 101,
            'desc': '--outfile-dir "{dir}" does not exist',
        },
        'NO_SUCH_MODE': {
            'code': 102,
            'desc': '{mode} is not a supported mode',
        },
        'OUTFILE_DIR_NOT_DIR': {
            'code': 103,
            'desc': '--outfile-dir "{dir}" is not a directory',
        },
        'DDL_FILE_LIST_NOT_SPECIFIED': {
            'code': 104,
            'desc': 'no ddl_file_list specified',
        },
        'UNABLE_TO_GET_DISK_SPACE': {
            'code': 105,
            'desc': 'Unable to read free disk size for path: {path}',
        },
        'FILE_ALREADY_EXIST': {
            'code': 106,
            'desc': (
                "Outfile {file} already exists. Please cleanup or use "
                "--force-cleanup if you are sure it's left behind by last "
                "unclean OSC stop"),
        },
        'DB_NOT_GIVEN': {
            'code': 110,
            'desc': ('At least one database name should be given for running '
                     'OSC'),
        },
        'DB_NOT_EXIST': {
            'code': 111,
            'desc': ('Database: {db_list} do(es) not exist in MySQL'),
        },
        'INVALID_SYNTAX': {
            'code': 112,
            'desc': (
                'Fail to parse: {filepath} {msg} '
                'Most likely is not a valid CREATE TABLE sql. '
                'Please make sure it has correct syntax and can be executed '
                'in MySQL: {msg}')
        },
        'INVALID_REPL_STATUS': {
            'code': 113,
            'desc': ('Invalid replication status: <{repl_status}>. '
                     '<master> and <slave> are the only supported ones'),
        },
        'FAILED_TO_LOCK': {
            'code': 115,
            'desc': ('Failed to grab external lock'),
        },
        'TOO_MANY_OSC_RUNNING': {
            'code': 116,
            'desc': ('Too many osc is running. {limit} allowed, {running} '
                     'running'),
        },
        'FAILED_TO_READ_DDL_FILE': {
            'code': 117,
            'desc': ("Failed to read DDL file: '{filepath}'"),
        },
        'ARGUMENT_ERROR': {
            'code': 118,
            'desc': (
                'Invalid value for argument {argu}: {errmsg}'),
        },
        'FAILED_TO_CONNECT_DB': {
            'code': 119,
            'desc': ('Failed to connect to database using user: {user} '
                     'through {socket}'),
        },
        'REPL_ROLE_MISMATCH': {
            'code': 120,
            'desc': ('Replication role fail to match what is given on CLI: '
                     '{given_role}'),
        },
        'FAILED_TO_FETCH_MYSQL_VARS': {
            'code': 121,
            'desc': ('Failed to fetch local mysql variables'),
        },
        'TABLE_ALREADY_EXIST': {
            'code': 122,
            'desc': ("Table `{db}`.`{table}` already exists in MySQL. "
                     "Please cleanup before run osc again"),
        },
        'TRIGGER_ALREADY_EXIST': {
            'code': 123,
            'desc': ("Following trigger(s) already exist on table: \n"
                     "{triggers}"),
        },
        'MISSING_COLUMN': {
            'code': 124,
            'desc': (
                'Column(s): {column} missing in new table schema '
                'specify --allow-drop-columns if you really want to drop '
                'the column'
            ),
        },
        'TABLE_NOT_EXIST': {
            'code': 125,
            'desc': ('Table: `{db}`.`{table}` does not exist in MySQL'),
        },
        'TABLE_PARSING_ERROR': {
            'code': 126,
            'desc': ('Fail to parse table: `{db}`.`{table}` {msg}'),
        },
        'NO_PK_EXIST': {
            'code': 127,
            'desc': ('Table: `{db}`.`{table}` does not have a primary key.'),
        },
        'NOT_ENOUGH_SPACE': {
            'code': 128,
            'desc': ('Not enough disk space to execute schema change. '
                     'Required: {need}, Available: {avail}'),
        },
        'DDL_GUARD_ATTEMPTS': {
            'code': 129,
            'desc': ("Max attempts exceeded, but the threads_running still "
                     "don't drop to an ideal number"),
        },
        'UNLOCK_FAILED': {
            'code': 130,
            'desc': (
                'Failed to unlock external lock'),
        },
        'OSC_INTERNAL_ERROR': {
            'code': 131,
            'desc': (
                'Internal OSC Exception: {msg}'),
        },
        'REPLAY_TIMEOUT': {
            'code': 132,
            'desc': ('Timeout when replaying changes'),
        },
        'REPLAY_WRONG_AFFECTED': {
            'code': 133,
            'desc': (
                'Unexpected affected number of rows when replaying events. '
                'This usually happens when application was writing to table '
                'without writing binlogs. For example `set session '
                'sql_log_bin=0` was executed before DML statements. '
                'Expected number: 1 row. Affected: {num}'
            ),
        },
        'CHECKSUM_MISMATCH': {
            'code': 134,
            'desc': (
                'Checksum mismatch between origin table and intermediate '
                'table. This means one of these: '
                '1. you have some scripts running DDL against the origin '
                'table while OSC is running. '
                '2. some columns have changed their output format, '
                'for example int -> decimal. '
                'see also: --skip-checksum-for-modifed '
                '3. it is a bug in OSC'
            ),
        },
        'OFFLINE_NOT_SUPPORTED': {
            'code': 135,
            'desc': ('--offline-checksum only supported in slave mode, '
                     'however replication is not running at the moment'),
        },
        'UNABLE_TO_GET_LOCK': {
            'code': 136,
            'desc': (
                'Unable to get MySQL lock for OSC. Please check whether there '
                'is another OSC job already running somewhere'),
        },
        'FAIL_TO_GUESS_CHUNK_SIZE': {
            'code': 137,
            'desc': ('Failed to decide optmial chunk size for dump'),
        },
        'NO_INDEX_COVERAGE': {
            'code': 138,
            'desc': ('None of the indexes in new table schema can perfectly '
                     'cover current pk combination lookup: <{pk_names}>. '
                     'Use --skip-pk-coverage-check, if you are sure it will '
                     'not cause a problem'),
        },
        'NEW_PK': {
            'code': 139,
            'desc': ("You're adding new primary key to table. This will "
                     "cause a long running transaction open during the data "
                     "dump stage. Specify --alow-new-pk if you don't think "
                     "this will be a performance issue for you")
        },
        'MAX_ATTEMPT_EXCEEDED': {
            'code': 140,
            'desc': ("Max attempt exceeded, but time spent in replay still "
                     "does not meet the requirement. We will not proceed. "
                     "There're probably too many write requests targeting the "
                     "table. If blocking the writes for more than {timeout} "
                     "seconds is not a problem for you, then specify "
                     "--bypass-replay-timeout")
        },
        'LONG_RUNNING_TRX': {
            'code': 141,
            'desc': ("Long running transaction exist: \n"
                     "ID: {pid}\n"
                     "User: {user}\n"
                     "host: {host}\n"
                     "Time: {time}\n"
                     "Command: {command}\n"
                     "Info: {info}\n")
        },
        'UNKOWN_REPLAY_TYPE': {
            'code': 142,
            'desc': ('Unknown replay type: {type_value}'),
        },
        'FAILED_TO_LOCK_TABLE': {
            'code': 143,
            'desc': ('Failed to lock table: {tables}'),
        },
        'FOREIGN_KEY_FOUND': {
            'code': 144,
            'desc': ("{db}.{table} is referencing or being referenced "
                     "in at least one foreign key")
        },
        'WRONG_ENGINE': {
            'code': 145,
            'desc': (
                'Engine in the SQL file "{engine}" does not match "{expect}" '
                'which is given on CLI'),
        },
        'PRI_COL_DROPPED': {
            'code': 146,
            'desc': ('<{pri_col}> which belongs to current primary key is '
                     'dropped in new schema. Dropping a column from current '
                     'primary key is dangerous, and can cause data loss. '
                     'Please separate into two OSC jobs if you really want to '
                     'perform this schema change. 1. move this column out of '
                     'current primary key. 2. drop this column after step1.')
        },
        'INCORRECT_SESSION_OVERRIDE': {
            'code': 147,
            'desc': ('Failed to parse the given session override '
                     'configuration. Failing part: {section}')
        },
        'NOT_RBR_SAFE': {
            'code': 148,
            'desc': (
                'Running OSC with RBR is not safe for a non-FB MySQL version. '
                'You will need to either have "sql_log_bin_triggers" '
                'supported and enabled, or disable RBR before running OSC'
            )
        },
        'ASSERTION_ERROR': {
            'code': 249,
            'desc': (
                "Assertion error. \n"
                "Expected: {expected}\n"
                "Got     : {got}"),
        },
        'CLEANUP_EXECUTION_ERROR': {
            'code': 250,
            'desc': (
                'Error when running clean up statement: {sql} msg: {msg}'),
        },
        'HOOK_EXECUTION_ERROR': {
            'code': 251,
            'desc': (
                'Error when executing hook: {hook} msg: {msg}'),
        },
        'SHELL_ERROR': {
            'code': 252,
            'desc': (
                'Shell command exit with error when executing: {cmd} '
                'STDERR: {stderr}'),
        },
        'SHELL_TIMEOUT': {
            'code': 253,
            'desc': ('Timeout when executing shell command: {cmd}'),
        },
        'GENERIC_MYSQL_ERROR': {
            'code': 254,
            'desc': (
                'MySQL Error during stage "{stage}": [{errnum}] {errmsg}'),
        },
    }

    def __init__(self, err_key, desc_kwargs=None, mysql_err_code=None):
        self.err_key = err_key
        if desc_kwargs:
            self.desc_kwargs = desc_kwargs
        else:
            self.desc_kwargs = {}
        self._mysql_err_code = mysql_err_code
        self.err_entry = self.ERR_MAPPING[err_key]

    @property
    def code(self):
        return self.ERR_MAPPING[self.err_key]['code']

    @property
    def desc(self):
        description = self.err_entry['desc'].format(**self.desc_kwargs)
        return '{}: {}'.format(self.err_key, description)

    @property
    def mysql_err_code(self):
        if self._mysql_err_code:
            return self._mysql_err_code
        else:
            return 0

    def __str__(self):
        return self.desc
