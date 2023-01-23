#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""


class OSCError(Exception):
    ERR_MAPPING = {
        "NON_ROOT_USER": {
            "code": 100,
            "desc": "Non-root user execution",
            "retryable": False,
        },
        "OUTFILE_DIR_NOT_EXIST": {
            "code": 101,
            "desc": '--outfile-dir "{dir}" does not exist',
            "retryable": False,
        },
        "NO_SUCH_MODE": {
            "code": 102,
            "desc": "{mode} is not a supported mode",
            "retryable": False,
        },
        "OUTFILE_DIR_NOT_DIR": {
            "code": 103,
            "desc": '--outfile-dir "{dir}" is not a directory',
            "retryable": False,
        },
        "DDL_FILE_LIST_NOT_SPECIFIED": {
            "code": 104,
            "desc": "no ddl_file_list specified",
            "retryable": False,
        },
        "UNABLE_TO_GET_FREE_DISK_SPACE": {
            "code": 105,
            "desc": "Unable to read free disk size for path: {path}",
            "retryable": True,
        },
        "FILE_ALREADY_EXIST": {
            "code": 106,
            "desc": (
                "Outfile {file} already exists. Please cleanup or use "
                "--force-cleanup if you are sure it's left behind by last "
                "unclean OSC stop"
            ),
            "retryable": False,
        },
        "UNABLE_TO_GET_PARTITION_SIZE": {
            "code": 107,
            "desc": "Unable to read partition size from path: {path}",
            "retryable": True,
        },
        "DB_NOT_GIVEN": {
            "code": 110,
            "desc": ("At least one database name should be given for running " "OSC"),
            "retryable": False,
        },
        "DB_NOT_EXIST": {
            "code": 111,
            "desc": ("Database: {db_list} do(es) not exist in MySQL"),
            "retryable": False,
        },
        "INVALID_SYNTAX": {
            "code": 112,
            "desc": (
                "Fail to parse: {filepath} {msg} "
                "Most likely is not a valid CREATE TABLE sql. "
                "Please make sure it has correct syntax and can be executed "
                "in MySQL: {msg}"
            ),
            "retryable": False,
        },
        "INVALID_REPL_STATUS": {
            "code": 113,
            "desc": (
                "Invalid replication status: <{repl_status}>. "
                "<master> and <slave> are the only supported ones"
            ),
            "retryable": False,
        },
        "FAILED_TO_LOCK": {
            "code": 115,
            "desc": (
                "Failed to grab external lock, "
                "existing locks that may block us: {locks}"
            ),
            "retryable": True,
        },
        "TOO_MANY_OSC_RUNNING": {
            "code": 116,
            "desc": ("Too many osc is running. {limit} allowed, {running} " "running"),
            "retryable": True,
        },
        "FAILED_TO_READ_DDL_FILE": {
            "code": 117,
            "desc": ("Failed to read DDL file: '{filepath}'"),
            "retryable": True,
        },
        "ARGUMENT_ERROR": {
            "code": 118,
            "desc": ("Invalid value for argument {argu}: {errmsg}"),
            "retryable": False,
        },
        "FAILED_TO_CONNECT_DB": {
            "code": 119,
            "desc": (
                "Failed to connect to database using user: {user} " "through {socket}"
            ),
            "retryable": True,
        },
        "REPL_ROLE_MISMATCH": {
            "code": 120,
            "desc": (
                "Replication role fail to match what is given on CLI: " "{given_role}"
            ),
            "retryable": False,
        },
        "FAILED_TO_FETCH_MYSQL_VARS": {
            "code": 121,
            "desc": ("Failed to fetch local mysql variables"),
            "retryable": True,
        },
        "TABLE_ALREADY_EXIST": {
            "code": 122,
            "desc": (
                "Table `{db}`.`{table}` already exists in MySQL. "
                "Please cleanup before run osc again"
            ),
            "retryable": False,
        },
        "TRIGGER_ALREADY_EXIST": {
            "code": 123,
            "desc": ("Following trigger(s) already exist on table: \n" "{triggers}"),
            "retryable": False,
        },
        "MISSING_COLUMN": {
            "code": 124,
            "desc": (
                "Column(s): {column} missing in new table schema "
                "specify --allow-drop-column if you really want to drop "
                "the column"
            ),
            "retryable": False,
        },
        "TABLE_NOT_EXIST": {
            "code": 125,
            "desc": ("Table: `{db}`.`{table}` does not exist in MySQL"),
            "retryable": False,
        },
        "TABLE_PARSING_ERROR": {
            "code": 126,
            "desc": ("Fail to parse table: `{db}`.`{table}` {msg}"),
            "retryable": False,
        },
        "NO_PK_EXIST": {
            "code": 127,
            "desc": ("Table: `{db}`.`{table}` does not have a primary key."),
            "retryable": False,
        },
        "NOT_ENOUGH_SPACE": {
            "code": 128,
            "desc": (
                "Not enough disk space to execute schema change. "
                "Required: {need}, Available: {avail}"
            ),
            "retryable": False,
        },
        "DDL_GUARD_ATTEMPTS": {
            "code": 129,
            "desc": (
                "Max attempts exceeded, but the threads_running still "
                "don't drop to an ideal number"
            ),
            "retryable": True,
        },
        "UNLOCK_FAILED": {
            "code": 130,
            "desc": ("Failed to unlock external lock"),
            "retryable": True,
        },
        "OSC_INTERNAL_ERROR": {
            "code": 131,
            "desc": ("Internal OSC Exception: {msg}"),
            "retryable": False,
        },
        "REPLAY_TIMEOUT": {
            "code": 132,
            "desc": ("Timeout when replaying changes"),
            "retryable": True,
        },
        "REPLAY_WRONG_AFFECTED": {
            "code": 133,
            "desc": (
                "Unexpected affected number of rows when replaying events. "
                "This usually happens when application was writing to table "
                "without writing binlogs. For example `set session "
                "sql_log_bin=0` was executed before DML statements. "
                "Expected number: 1 row. Affected: {num}"
            ),
            "retryable": False,
        },
        "CHECKSUM_MISMATCH": {
            "code": 134,
            "desc": (
                "Checksum mismatch between origin table and intermediate "
                "table. This means one of these: "
                "1. you have some scripts running DDL against the origin "
                "table while OSC is running. "
                "2. some columns have changed their output format, "
                "for example int -> decimal. "
                "see also: --skip-checksum-for-modified "
                "3. it is a bug in OSC"
            ),
            "retryable": True,
        },
        "OFFLINE_NOT_SUPPORTED": {
            "code": 135,
            "desc": (
                "--offline-checksum only supported in slave mode, "
                "however replication is not running at the moment"
            ),
            "retryable": False,
        },
        "UNABLE_TO_GET_LOCK": {
            "code": 136,
            "desc": (
                "Unable to get MySQL lock for OSC. Please check whether there "
                "is another OSC job already running somewhere. Use `cleanup "
                "--kill` subcommand to kill the running job if you are not "
                "interested in it anymore"
            ),
            "retryable": True,
        },
        "FAIL_TO_GUESS_CHUNK_SIZE": {
            "code": 137,
            "desc": ("Failed to decide optimal chunk size for dump"),
            "retryable": True,
        },
        "NO_INDEX_COVERAGE": {
            "code": 138,
            "desc": (
                "None of the indexes in new table schema can perfectly "
                "cover current pk combination lookup: <{pk_names}>. "
                "Use --skip-pk-coverage-check, if you are sure it will "
                "not cause a problem"
            ),
            "retryable": False,
        },
        "NEW_PK": {
            "code": 139,
            "desc": (
                "You're adding new primary key to table. This will "
                "cause a long running transaction open during the data "
                "dump stage. Specify --allow-new-pk if you don't think "
                "this will be a performance issue for you"
            ),
            "retryable": False,
        },
        "MAX_ATTEMPT_EXCEEDED": {
            "code": 140,
            "desc": (
                "Max attempt exceeded, but time spent in replay still "
                "does not meet the requirement. We will not proceed. "
                "There're probably too many write requests targeting the "
                "table. If blocking the writes for more than {timeout} "
                "seconds is not a problem for you, then specify "
                "--bypass-replay-timeout"
            ),
            "retryable": True,
        },
        "LONG_RUNNING_TRX": {
            "code": 141,
            "desc": (
                "Long running transaction exist: \n"
                "ID: {pid}\n"
                "User: {user}\n"
                "host: {host}\n"
                "Time: {time}\n"
                "Command: {command}\n"
                "Info: {info}\n"
            ),
            "retryable": True,
        },
        "UNKOWN_REPLAY_TYPE": {
            "code": 142,
            "desc": ("Unknown replay type: {type_value}"),
            "retryable": False,
        },
        "FAILED_TO_LOCK_TABLE": {
            "code": 143,
            "desc": ("Failed to lock table: {tables}"),
            "retryable": True,
        },
        "FOREIGN_KEY_FOUND": {
            "code": 144,
            "desc": (
                "{db}.{table} is referencing or being referenced "
                "in at least one foreign key: "
                "{fk}"
            ),
            "retryable": False,
        },
        "WRONG_ENGINE": {
            "code": 145,
            "desc": (
                'Engine in the SQL file "{engine}" does not match "{expect}" '
                "which is given on CLI"
            ),
            "retryable": False,
        },
        "PRI_COL_DROPPED": {
            "code": 146,
            "desc": (
                "<{pri_col}> which belongs to current primary key is "
                "dropped in new schema. Dropping a column from current "
                "primary key is dangerous, and can cause data loss. "
                "Please separate into two OSC jobs if you really want to "
                "perform this schema change. 1. move this column out of "
                "current primary key. 2. drop this column after step1."
            ),
            "retryable": False,
        },
        "INCORRECT_SESSION_OVERRIDE": {
            "code": 147,
            "desc": (
                "Failed to parse the given session override "
                "configuration. Failing part: {section}"
            ),
            "retryable": False,
        },
        "NOT_RBR_SAFE": {
            "code": 148,
            "desc": (
                "Running OSC with RBR is not safe for a non-FB MySQL version. "
                'You will need to either have "sql_log_bin_triggers" '
                "supported and enabled, or disable RBR before running OSC"
            ),
            "retryable": False,
        },
        "IMPLICIT_CONVERSION_DETECTED": {
            "code": 149,
            "desc": (
                "Implicit conversion happened after executing the CREATE "
                "TABLE statement. It is a best practice to always store your "
                "schema in a consistent way. Please make sure that the "
                "statement provided in the file is copied from the output of "
                "`SHOW CREATE TABLE`. Difference detected: \n {diff}"
            ),
            "retryable": False,
        },
        "FAILED_TO_DECODE_DDL_FILE": {
            "code": 150,
            "desc": (
                "Failed to decode DDL file '{filepath}' "
                "with charset '{charset}'. Use --charset "
                "to set the proper charset."
            ),
            "retryable": False,
        },
        "REPLAY_TOO_MANY_DELTAS": {
            "code": 151,
            "desc": (
                "Recorded too many changes to ever catchup "
                "({deltas} > max replay changes {max_deltas})"
            ),
            "retryable": True,
        },
        "UNSAFE_TS_BOOTSTRAP": {
            "code": 152,
            "desc": (
                "Adding columns or changing columns to use CURRENT_TIMESTAMP as "
                "default value is unsafe with OSC. Please consider a different "
                "deployment method for this"
            ),
            "retryable": False,
        },
        "CREATE_TRIGGER_ERROR": {
            "code": 153,
            "desc": ("Error when creating triggers, msg: {msg}"),
            "retryable": True,
        },
        # reserved for special internal errors
        "ASSERTION_ERROR": {
            "code": 249,
            "desc": ("Assertion error. \n" "Expected: {expected}\n" "Got     : {got}"),
            "retryable": False,
        },
        "CLEANUP_EXECUTION_ERROR": {
            "code": 250,
            "desc": ("Error when running clean up statement: {sql} msg: {msg}"),
            "retryable": True,
        },
        "HOOK_EXECUTION_ERROR": {
            "code": 251,
            "desc": ("Error when executing hook: {hook} msg: {msg}"),
            "retryable": True,
        },
        "SHELL_ERROR": {
            "code": 252,
            "desc": (
                "Shell command exit with error when executing: {cmd} "
                "STDERR: {stderr}"
            ),
            "retryable": True,
        },
        "SHELL_TIMEOUT": {
            "code": 253,
            "desc": ("Timeout when executing shell command: {cmd}"),
            "retryable": True,
        },
        "GENERIC_MYSQL_ERROR": {
            "code": 254,
            "desc": ('MySQL Error during stage "{stage}": [{errnum}] {errmsg}'),
            "retryable": True,
        },
        "OUTFILE_DIR_NOT_SPECIFIED_WSENV": {
            "code": 255,
            "desc": ("--outfile-dir must be specified when using wsenv"),
            "retryable": False,
        },
        "SKIP_DISK_SPACE_CHECK_VALUE_INCOMPATIBLE_WSENV": {
            "code": 256,
            "desc": ("-skip-disk-space-check must be true when using wsenv"),
            "retryable": False,
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
        self._retryable = self.ERR_MAPPING[err_key]["retryable"]

    @property
    def code(self):
        return self.ERR_MAPPING[self.err_key]["code"]

    @property
    def desc(self):
        description = self.err_entry["desc"].format(**self.desc_kwargs)
        return "{}: {}: {}".format(self.code, self.err_key, description)

    @property
    def mysql_err_code(self):
        if self._mysql_err_code:
            return self._mysql_err_code
        else:
            return 0

    @property
    def retryable(self):
        return self._retryable

    def __str__(self):
        return self.desc
