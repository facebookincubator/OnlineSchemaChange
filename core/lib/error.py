#!/usr/bin/env python3

"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

from enum import auto, IntEnum, unique
from typing import NamedTuple


class ErrorDescriptor(NamedTuple):
    code: int
    desc: str
    retryable: bool
    internal: bool


class OSCError(Exception):
    @unique
    class Errors(IntEnum):
        NON_ROOT_USER = 100
        OUTFILE_DIR_NOT_EXIST = auto()
        NO_SUCH_MODE = auto()
        OUTFILE_DIR_NOT_DIR = auto()
        DDL_FILE_LIST_NOT_SPECIFIED = auto()
        UNABLE_TO_GET_FREE_DISK_SPACE = auto()
        FILE_ALREADY_EXIST = auto()
        UNABLE_TO_GET_PARTITION_SIZE = auto()
        DB_NOT_GIVEN = auto()
        DB_NOT_EXIST = auto()
        INVALID_SYNTAX = auto()
        INVALID_REPL_STATUS = auto()
        FAILED_TO_LOCK = auto()
        TOO_MANY_OSC_RUNNING = auto()
        FAILED_TO_READ_DDL_FILE = auto()
        ARGUMENT_ERROR = auto()
        FAILED_TO_CONNECT_DB = auto()
        REPL_ROLE_MISMATCH = auto()
        FAILED_TO_FETCH_MYSQL_VARS = auto()
        TABLE_ALREADY_EXIST = auto()
        TRIGGER_ALREADY_EXIST = auto()
        MISSING_COLUMN = auto()
        TABLE_NOT_EXIST = auto()
        TABLE_PARSING_ERROR = auto()
        NO_PK_EXIST = auto()
        NOT_ENOUGH_SPACE = auto()
        DDL_GUARD_ATTEMPTS = auto()
        UNLOCK_FAILED = auto()
        OSC_INTERNAL_ERROR = auto()
        REPLAY_TIMEOUT = auto()
        REPLAY_WRONG_AFFECTED = auto()
        CHECKSUM_MISMATCH = auto()
        OFFLINE_NOT_SUPPORTED = auto()
        UNABLE_TO_GET_LOCK = auto()
        FAIL_TO_GUESS_CHUNK_SIZE = auto()
        NO_INDEX_COVERAGE = auto()
        NEW_PK = auto()
        MAX_ATTEMPT_EXCEEDED = auto()
        LONG_RUNNING_TRX = auto()
        UNKOWN_REPLAY_TYPE = auto()
        FAILED_TO_LOCK_TABLE = auto()
        FOREIGN_KEY_FOUND = auto()
        WRONG_ENGINE = auto()
        PRI_COL_DROPPED = auto()
        INCORRECT_SESSION_OVERRIDE = auto()
        NOT_RBR_SAFE = auto()
        IMPLICIT_CONVERSION_DETECTED = auto()
        FAILED_TO_DECODE_DDL_FILE = auto()
        REPLAY_TOO_MANY_DELTAS = auto()
        UNSAFE_TS_BOOTSTRAP = auto()
        CREATE_TRIGGER_ERROR = auto()
        MYROCKS_REQUIRED = auto()
        TABLE_TIMESTAMP_CHANGED_ERROR = auto()
        ASSERTION_ERROR = auto()
        CLEANUP_EXECUTION_ERROR = auto()
        HOOK_EXECUTION_ERROR = auto()
        SHELL_ERROR = auto()
        SHELL_TIMEOUT = auto()
        GENERIC_MYSQL_ERROR = auto()
        OUTFILE_DIR_NOT_SPECIFIED_WSENV = auto()
        SKIP_DISK_SPACE_CHECK_VALUE_INCOMPATIBLE_WSENV = auto()
        OSC_CANNOT_MATCH_WRITE_RATE = auto()
        GENERIC_RETRYABLE_EXCEPTION = auto()
        GENERIC_NONRETRY_EXCEPTION = auto()
        NOT_IMPLEMENTED_EXCEPTION = auto()

    ERR_MAPPING: dict[str, ErrorDescriptor] = {
        Errors.NON_ROOT_USER.name: ErrorDescriptor(
            code=Errors.NON_ROOT_USER.value,
            desc="Non-root user execution",
            retryable=False,
            internal=True,
        ),
        Errors.OUTFILE_DIR_NOT_EXIST.name: ErrorDescriptor(
            code=Errors.OUTFILE_DIR_NOT_EXIST.value,
            desc='--outfile-dir "{dir}" does not exist',
            retryable=False,
            internal=True,
        ),
        Errors.NO_SUCH_MODE.name: ErrorDescriptor(
            code=Errors.NO_SUCH_MODE.value,
            desc="{mode} is not a supported mode",
            retryable=False,
            internal=True,
        ),
        Errors.OUTFILE_DIR_NOT_DIR.name: ErrorDescriptor(
            code=Errors.OUTFILE_DIR_NOT_DIR.value,
            desc='--outfile-dir "{dir}" is not a directory',
            retryable=False,
            internal=True,
        ),
        Errors.DDL_FILE_LIST_NOT_SPECIFIED.name: ErrorDescriptor(
            code=Errors.DDL_FILE_LIST_NOT_SPECIFIED.value,
            desc="no ddl_file_list specified",
            retryable=False,
            internal=True,
        ),
        Errors.UNABLE_TO_GET_FREE_DISK_SPACE.name: ErrorDescriptor(
            code=Errors.UNABLE_TO_GET_FREE_DISK_SPACE.value,
            desc="Unable to read free disk size for path: {path}",
            retryable=True,
            internal=True,
        ),
        Errors.FILE_ALREADY_EXIST.name: ErrorDescriptor(
            code=Errors.FILE_ALREADY_EXIST.value,
            desc=(
                "Outfile {file} already exists. Please cleanup or use "
                "--force-cleanup if you are sure it's left behind by last "
                "unclean OSC stop"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.UNABLE_TO_GET_PARTITION_SIZE.name: ErrorDescriptor(
            code=Errors.UNABLE_TO_GET_PARTITION_SIZE.value,
            desc="Unable to read partition size from path: {path}",
            retryable=True,
            internal=True,
        ),
        Errors.DB_NOT_GIVEN.name: ErrorDescriptor(
            code=Errors.DB_NOT_GIVEN.value,
            desc=("At least one database name should be given for running " "OSC"),
            retryable=False,
            internal=True,
        ),
        Errors.DB_NOT_EXIST.name: ErrorDescriptor(
            code=Errors.DB_NOT_EXIST.value,
            desc=("Database: {db_list} do(es) not exist in MySQL"),
            retryable=False,
            internal=True,
        ),
        Errors.INVALID_SYNTAX.name: ErrorDescriptor(
            code=Errors.INVALID_SYNTAX.value,
            desc=(
                "Fail to parse: {filepath} {msg} "
                "Most likely is not a valid CREATE TABLE sql. "
                "Please make sure it has correct syntax and can be executed "
                "in MySQL: {msg}"
            ),
            retryable=False,
            internal=True,  # Shouldn't hit parse error on desired schema in OSC
        ),
        Errors.INVALID_REPL_STATUS.name: ErrorDescriptor(
            code=Errors.INVALID_REPL_STATUS.value,
            desc=(
                "Invalid replication status: <{repl_status}>. "
                "<master> and <slave> are the only supported ones"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.FAILED_TO_LOCK.name: ErrorDescriptor(
            code=Errors.FAILED_TO_LOCK.value,
            desc=(
                "Failed to grab external lock, "
                "existing locks that may block us: {locks}. We serialize schema change operations since these consume significant machine resources on instance/host and can impact user workload if executed in parallel. Please use the wiki linked above to search for additional debug information about the blocking lock name[search for the name in the wiki]."
            ),
            retryable=True,
            internal=True,
        ),
        Errors.TOO_MANY_OSC_RUNNING.name: ErrorDescriptor(
            code=Errors.TOO_MANY_OSC_RUNNING.value,
            desc=("Too many osc is running. {limit} allowed, {running} " "running"),
            retryable=True,
            internal=True,
        ),
        Errors.FAILED_TO_READ_DDL_FILE.name: ErrorDescriptor(
            code=Errors.FAILED_TO_READ_DDL_FILE.value,
            desc=("Failed to read DDL file: '{filepath}'"),
            retryable=True,
            internal=True,
        ),
        Errors.ARGUMENT_ERROR.name: ErrorDescriptor(
            code=Errors.ARGUMENT_ERROR.value,
            desc=("Invalid value for argument {argu}: {errmsg}"),
            retryable=False,
            internal=True,
        ),
        Errors.FAILED_TO_CONNECT_DB.name: ErrorDescriptor(
            code=Errors.FAILED_TO_CONNECT_DB.value,
            desc=(
                "Failed to connect to database using user: {user} " "through {socket}"
            ),
            retryable=True,
            internal=True,
        ),
        Errors.REPL_ROLE_MISMATCH.name: ErrorDescriptor(
            code=Errors.REPL_ROLE_MISMATCH.value,
            desc=(
                "Replication role fail to match what is given on CLI: " "{given_role}"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.FAILED_TO_FETCH_MYSQL_VARS.name: ErrorDescriptor(
            code=Errors.FAILED_TO_FETCH_MYSQL_VARS.value,
            desc=("Failed to fetch local mysql variables"),
            retryable=True,
            internal=True,
        ),
        Errors.TABLE_ALREADY_EXIST.name: ErrorDescriptor(
            code=Errors.TABLE_ALREADY_EXIST.value,
            desc=(
                "Table `{db}`.`{table}` already exists in MySQL. "
                "Please cleanup before run osc again"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.TRIGGER_ALREADY_EXIST.name: ErrorDescriptor(
            code=Errors.TRIGGER_ALREADY_EXIST.value,
            desc=("Following trigger(s) already exist on table: \n" "{triggers}"),
            retryable=False,
            internal=True,
        ),
        Errors.MISSING_COLUMN.name: ErrorDescriptor(
            code=Errors.MISSING_COLUMN.value,
            desc=(
                "Column(s): {column} missing in new table schema "
                "specify --allow-drop-column if you really want to drop "
                "the column"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.TABLE_NOT_EXIST.name: ErrorDescriptor(
            code=Errors.TABLE_NOT_EXIST.value,
            desc=("Table: `{db}`.`{table}` does not exist in MySQL"),
            retryable=False,
            internal=True,
        ),
        Errors.TABLE_PARSING_ERROR.name: ErrorDescriptor(
            code=Errors.TABLE_PARSING_ERROR.value,
            desc=("Fail to parse table: `{db}`.`{table}` {msg}"),
            retryable=False,
            internal=True,
            # Parser may fail on original schema
            # but users need support for this case
        ),
        Errors.NO_PK_EXIST.name: ErrorDescriptor(
            code=Errors.NO_PK_EXIST.value,
            desc=("Table: `{db}`.`{table}` does not have a primary key."),
            retryable=False,
            internal=False,
        ),
        Errors.NOT_ENOUGH_SPACE.name: ErrorDescriptor(
            code=Errors.NOT_ENOUGH_SPACE.value,
            desc=(
                "Not enough disk space to execute schema change. "
                "Required: {need}, Available: {avail}"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.DDL_GUARD_ATTEMPTS.name: ErrorDescriptor(
            code=Errors.DDL_GUARD_ATTEMPTS.value,
            desc=(
                "Max attempts exceeded, but the threads_running still "
                "don't drop to an ideal number"
            ),
            retryable=True,
            internal=True,
        ),
        Errors.UNLOCK_FAILED.name: ErrorDescriptor(
            code=Errors.UNLOCK_FAILED.value,
            desc=("Failed to unlock external lock"),
            retryable=True,
            internal=True,
        ),
        Errors.OSC_INTERNAL_ERROR.name: ErrorDescriptor(
            code=Errors.OSC_INTERNAL_ERROR.value,
            desc=("Internal OSC Exception: {msg}"),
            retryable=False,
            internal=True,
        ),
        Errors.REPLAY_TIMEOUT.name: ErrorDescriptor(
            code=Errors.REPLAY_TIMEOUT.value,
            desc=("Timeout when replaying changes"),
            retryable=True,
            internal=True,
        ),
        Errors.REPLAY_WRONG_AFFECTED.name: ErrorDescriptor(
            code=Errors.REPLAY_WRONG_AFFECTED.value,
            desc=(
                "Unexpected affected number of rows when replaying events. "
                "This usually happens when application was writing to table "
                "without writing binlogs. For example `set session "
                "sql_log_bin=0` was executed before DML statements. "
                "Expected number: 1 row. Affected: {num}"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.CHECKSUM_MISMATCH.name: ErrorDescriptor(
            code=Errors.CHECKSUM_MISMATCH.value,
            desc=(
                "Checksum mismatch between origin table and intermediate "
                "table. This means one of these: "
                "1. you have some scripts running DDL against the origin "
                "table while OSC is running. "
                "2. some columns have changed their output format, "
                "for example int -> decimal. "
                "see also: --skip-checksum-for-modified "
                "3. it is a bug in OSC"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.OFFLINE_NOT_SUPPORTED.name: ErrorDescriptor(
            code=Errors.OFFLINE_NOT_SUPPORTED.value,
            desc=(
                "--offline-checksum only supported in slave mode, "
                "however replication is not running at the moment"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.UNABLE_TO_GET_LOCK.name: ErrorDescriptor(
            code=Errors.UNABLE_TO_GET_LOCK.value,
            desc=(
                "Unable to get MySQL lock for OSC. Please check whether there "
                "is another OSC job already running somewhere. Use `cleanup "
                "--kill` subcommand to kill the running job if you are not "
                "interested in it anymore"
            ),
            retryable=True,
            internal=True,
        ),
        Errors.FAIL_TO_GUESS_CHUNK_SIZE.name: ErrorDescriptor(
            code=Errors.FAIL_TO_GUESS_CHUNK_SIZE.value,
            desc=("Failed to decide optimal chunk size for dump"),
            retryable=True,
            internal=True,
        ),
        Errors.NO_INDEX_COVERAGE.name: ErrorDescriptor(
            code=Errors.NO_INDEX_COVERAGE.value,
            desc=(
                "None of the indexes in new table schema can perfectly "
                "cover current pk combination lookup: <{pk_names}>. "
                "Use --skip-pk-coverage-check, if you are sure it will "
                "not cause a problem"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.NEW_PK.name: ErrorDescriptor(
            code=Errors.NEW_PK.value,
            desc=(
                "You're adding new primary key to table. This will "
                "cause a long running transaction open during the data "
                "dump stage. Specify --allow-new-pk if you don't think "
                "this will be a performance issue for you"
            ),
            retryable=False,
            internal=False,
        ),
        Errors.MAX_ATTEMPT_EXCEEDED.name: ErrorDescriptor(
            code=Errors.MAX_ATTEMPT_EXCEEDED.value,
            desc=(
                "Max attempt exceeded, but time spent in replay still "
                "does not meet the requirement. We will not proceed. "
                "There're probably too many write requests targeting the "
                "table. If blocking the writes for more than {timeout} "
                "seconds is not a problem for you, then specify "
                "--bypass-replay-timeout"
            ),
            retryable=True,
            internal=True,
        ),
        Errors.LONG_RUNNING_TRX.name: ErrorDescriptor(
            code=Errors.LONG_RUNNING_TRX.value,
            desc=(
                "Long running transaction exist: \n"
                "ID: {pid}\n"
                "User: {user}\n"
                "host: {host}\n"
                "Time: {time}\n"
                "Command: {command}\n"
                "Info: {info}\n"
            ),
            retryable=True,
            internal=True,
        ),
        Errors.UNKOWN_REPLAY_TYPE.name: ErrorDescriptor(
            code=Errors.UNKOWN_REPLAY_TYPE.value,
            desc=("Unknown replay type: {type_value}"),
            retryable=False,
            internal=True,
        ),
        Errors.FAILED_TO_LOCK_TABLE.name: ErrorDescriptor(
            code=Errors.FAILED_TO_LOCK_TABLE.value,
            desc=("Failed to lock table: {tables}"),
            retryable=True,
            internal=True,
        ),
        Errors.FOREIGN_KEY_FOUND.name: ErrorDescriptor(
            code=Errors.FOREIGN_KEY_FOUND.value,
            desc=(
                "{db}.{table} is referencing or being referenced "
                "in at least one foreign key: "
                "{fk}"
            ),
            retryable=False,
            internal=False,
        ),
        Errors.WRONG_ENGINE.name: ErrorDescriptor(
            code=Errors.WRONG_ENGINE.value,
            desc=(
                'Engine in the SQL file "{engine}" does not match "{expect}" '
                "which is given on CLI"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.PRI_COL_DROPPED.name: ErrorDescriptor(
            code=Errors.PRI_COL_DROPPED.value,
            desc=(
                "<{pri_col}> which belongs to current primary key is "
                "dropped in new schema. Dropping a column from current "
                "primary key is dangerous, and can cause data loss. "
                "Please separate into two OSC jobs if you really want to "
                "perform this schema change. 1. move this column out of "
                "current primary key. 2. drop this column after step1."
            ),
            retryable=False,
            internal=False,
        ),
        Errors.INCORRECT_SESSION_OVERRIDE.name: ErrorDescriptor(
            code=Errors.INCORRECT_SESSION_OVERRIDE.value,
            desc=(
                "Failed to parse the given session override "
                "configuration. Failing part: {section}"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.NOT_RBR_SAFE.name: ErrorDescriptor(
            code=Errors.NOT_RBR_SAFE.value,
            desc=(
                "Running OSC with RBR is not safe for a non-FB MySQL version. "
                'You will need to either have "sql_log_bin_triggers" '
                "supported and enabled, or disable RBR before running OSC"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.IMPLICIT_CONVERSION_DETECTED.name: ErrorDescriptor(
            code=Errors.IMPLICIT_CONVERSION_DETECTED.value,
            desc=(
                "Implicit conversion happened after executing the CREATE "
                "TABLE statement. It is a best practice to always store your "
                "schema in a consistent way. Please make sure that the "
                "statement provided in the file is copied from the output of "
                "`SHOW CREATE TABLE`. Difference detected: \n {diff}"
            ),
            retryable=False,
            internal=True,  # Shouldn't hit parse error on the desired schema in OSC
        ),
        Errors.FAILED_TO_DECODE_DDL_FILE.name: ErrorDescriptor(
            code=Errors.FAILED_TO_DECODE_DDL_FILE.value,
            desc=(
                "Failed to decode DDL file '{filepath}' "
                "with charset '{charset}'. Use --charset "
                "to set the proper charset."
            ),
            retryable=False,
            internal=True,
        ),
        Errors.REPLAY_TOO_MANY_DELTAS.name: ErrorDescriptor(
            code=Errors.REPLAY_TOO_MANY_DELTAS.value,
            desc=(
                "Recorded too many changes to ever catchup "
                "({deltas} > max replay changes {max_deltas})"
            ),
            retryable=True,
            internal=True,
        ),
        Errors.UNSAFE_TS_BOOTSTRAP.name: ErrorDescriptor(
            code=Errors.UNSAFE_TS_BOOTSTRAP.value,
            desc=(
                "Adding columns or changing columns to use CURRENT_TIMESTAMP as "
                "default value is unsafe with OSC. Please consider a different "
                "deployment method for this"
            ),
            retryable=False,
            internal=True,
        ),
        Errors.CREATE_TRIGGER_ERROR.name: ErrorDescriptor(
            code=Errors.CREATE_TRIGGER_ERROR.value,
            desc="Error when creating triggers, msg: {msg}",
            retryable=True,
            internal=True,
        ),
        Errors.MYROCKS_REQUIRED.name: ErrorDescriptor(
            code=Errors.MYROCKS_REQUIRED.value,
            desc="MyRocks required due to: {reason}",
            retryable=False,
            internal=False,
        ),
        Errors.TABLE_TIMESTAMP_CHANGED_ERROR.name: ErrorDescriptor(
            code=Errors.TABLE_TIMESTAMP_CHANGED_ERROR.value,
            desc=(
                "Table timestamp changed during copy phase.\nExpected: {expected}\nGot: {got}"
            ),
            retryable=True,
            internal=True,
        ),
        # reserved for special internal errors
        Errors.ASSERTION_ERROR.name: ErrorDescriptor(
            code=Errors.ASSERTION_ERROR.value,
            desc="Assertion error.\nExpected: {expected}\n" "Got: {got}",
            retryable=False,
            internal=True,
        ),
        Errors.CLEANUP_EXECUTION_ERROR.name: ErrorDescriptor(
            code=Errors.CLEANUP_EXECUTION_ERROR.value,
            desc="Error when running clean up statement: {sql} msg: {msg}",
            retryable=True,
            internal=True,
        ),
        Errors.HOOK_EXECUTION_ERROR.name: ErrorDescriptor(
            code=Errors.HOOK_EXECUTION_ERROR.value,
            desc="Error when executing hook: {hook} msg: {msg}",
            retryable=True,
            internal=True,
        ),
        Errors.SHELL_ERROR.name: ErrorDescriptor(
            code=Errors.SHELL_ERROR.value,
            desc=(
                "Shell command exit with error when executing: {cmd} "
                "STDERR: {stderr}"
            ),
            retryable=True,
            internal=True,
        ),
        Errors.SHELL_TIMEOUT.name: ErrorDescriptor(
            code=Errors.SHELL_TIMEOUT.value,
            desc=("Timeout when executing shell command: {cmd}"),
            retryable=True,
            internal=True,
        ),
        Errors.GENERIC_MYSQL_ERROR.name: ErrorDescriptor(
            code=Errors.GENERIC_MYSQL_ERROR.value,
            desc='MySQL Error during stage "{stage}": [{errnum}] {errmsg}',
            retryable=True,
            internal=True,
        ),
        Errors.OUTFILE_DIR_NOT_SPECIFIED_WSENV.name: ErrorDescriptor(
            code=Errors.OUTFILE_DIR_NOT_SPECIFIED_WSENV.value,
            desc="--outfile-dir must be specified when using wsenv",
            retryable=False,
            internal=True,
        ),
        Errors.SKIP_DISK_SPACE_CHECK_VALUE_INCOMPATIBLE_WSENV.name: ErrorDescriptor(
            code=Errors.SKIP_DISK_SPACE_CHECK_VALUE_INCOMPATIBLE_WSENV.value,
            desc="-skip-disk-space-check must be true when using wsenv",
            retryable=False,
            internal=True,
        ),
        Errors.OSC_CANNOT_MATCH_WRITE_RATE.name: ErrorDescriptor(
            code=Errors.OSC_CANNOT_MATCH_WRITE_RATE.value,
            desc="OSC catchup speed {speed} is not matching the write rate. We have exhausted the retries. Please reduce the incoming write rate or use a different deployment method for this(check documentation before proceeding)",
            retryable=False,
            internal=True,
        ),
        Errors.GENERIC_RETRYABLE_EXCEPTION.name: ErrorDescriptor(
            code=Errors.GENERIC_RETRYABLE_EXCEPTION.value,
            desc="{errmsg}",
            retryable=True,
            internal=False,
        ),
        Errors.GENERIC_NONRETRY_EXCEPTION.name: ErrorDescriptor(
            code=Errors.GENERIC_NONRETRY_EXCEPTION.value,
            desc="{errmsg}",
            retryable=False,
            internal=False,
        ),
        Errors.NOT_IMPLEMENTED_EXCEPTION.name: ErrorDescriptor(
            code=Errors.NOT_IMPLEMENTED_EXCEPTION.value,
            desc=("{errmsg}"),
            retryable=False,
            internal=True,
        ),
    }

    def __init__(
        self, err_key: str, desc_kwargs=None, mysql_err_code: int | None = None
    ):
        self.err_key = err_key
        if desc_kwargs:
            self.desc_kwargs = desc_kwargs
        else:
            self.desc_kwargs = {}
        self._mysql_err_code = mysql_err_code
        self.err_entry = self.ERR_MAPPING[err_key]
        self._retryable: bool = self.err_entry.retryable

    @property
    def code(self) -> int:
        return self.err_entry.code

    @property
    def desc(self) -> str:
        description = self.err_entry.desc.format(**self.desc_kwargs)
        return "{}: {}: {}".format(self.code, self.err_key, description)

    @property
    def mysql_err_code(self) -> int:
        return self._mysql_err_code or 0

    @property
    def retryable(self) -> bool:
        return self._retryable

    def __str__(self) -> str:
        return self.desc
