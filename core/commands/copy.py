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

from .base import CommandBase
from ..lib import util
from ..lib import constant
from ..lib.error import OSCError
from ..lib.payload.copy import CopyPayload

log = logging.getLogger(__name__)


class Copy(CommandBase):
    DESCRIPTION = (
        "Copy mode. In this mode, a temporary table with new schema will be "
        "created and filled with data. Finally there will be a table name "
        "swap to make the change take effect.\n"
    )
    NAME = 'copy'

    def setup_optional_args(self, parser):
        parser.add_argument("--rebuild", action='store_true',
                            help="Force an OSC operation even current table "
                            "already has the desired schema")
        parser.add_argument("--skip-pk-coverage-check", action='store_true',
                            help="Skip the PK coverage check which ensures "
                            "at least one index in the new schema can cover "
                            "the current PK combination for row lookup.")
        parser.add_argument("--rm-partition", action='store_true',
                            help="Ignore the partition scheme from the DDL "
                            "SQL. Useful if you want OSC to use "
                            "your current partition schemes.")
        parser.add_argument("--eliminate-dups", action='store_true',
                            help="Removes duplicate entries for PK/uniques"
                            "Could break replication if not run on master "
                            "first")
        parser.add_argument("--detailed-mismatch-info", action='store_true',
                            help="Use a slower but more accurate checksum "
                            "output if there's a checksum mismatch")
        parser.add_argument("--dump-after-checksum", action='store_true',
                            help="Dump the data onto disk after calculating "
                            "checksum for each chunk. Use this when you're "
                            "investigating a checksum mismatch issue")
        parser.add_argument("--allow-new-pk", action='store_true',
                            help="Allow adding primary key to a table, which "
                            "don't have PK yet. This will result in a "
                            "transaction being opened for a long time during "
                            "SELECT INTO OUTFILE stage. Specify this option "
                            "if you think this won't be an issue")
        parser.add_argument("--allow-drop-column", action='store_true',
                            help="Allow a column to be droppped if it does not"
                            "exist in the new schema")
        parser.add_argument("--idx-recreation", action='store_true',
                            help="Drop non-unique indexes before loading "
                            "data, and recreate them afterwards. This can "
                            "prevent index fragmentation, but will also slow "
                            "down the whole process slower")
        parser.add_argument("--force-cleanup", action='store_true',
                            help="Force cleanup before executing DDL."
                            "This is useful when you are sure there's no "
                            "other copy of OSC running on the same instance, "
                            "and want to ignore garbage left by last run")
        parser.add_argument("--outfile-dir",
                            help="Directory where we temporarily store data"
                            "for 'select into outfile'. data_dir will be used"
                            "if not specified here")
        parser.add_argument("--pre-load-statement",
                            help="SQL to be executed before loading data into "
                            "new table. You may want lower down durability to "
                            "to speed up the load, though not recommended")
        parser.add_argument("--post-load-statement",
                            help="SQL to be executed after loading data into "
                            "new table. This is useful when you've specified "
                            "--pre-load-statement for some temporary tunning "
                            "before load and want to revert after loading")
        parser.add_argument("--replay-timeout", type=int, default=30,
                            help="Maximum time in seconds for the final "
                            "catchup replay. As we hold write lock during "
                            "the period, we don't want to block writes for so "
                            "long")
        parser.add_argument("--replay-batch-size", type=int,
                            default=constant.DEFAULT_BATCH_SIZE,
                            help="Commit transaction after X changes have "
                            "been replayed")
        parser.add_argument("--replay-grouping-size", type=int,
                            default=constant.DEFAULT_REPLAY_GROUP_SIZE,
                            help="Do not group more than this number of "
                            "events with the same type into one replay query")
        parser.add_argument("--replay-max-attempt", type=int,
                            default=constant.DEFAULT_REPLAY_ATTEMPT,
                            help="Maximum number of times we should try to "
                            "replay changes before we decide it's impossible "
                            "to catch up.")
        parser.add_argument("--reserved-space",
                            default=constant.DEFAULT_RESERVED_SPACE, type=int,
                            help="Keep --outfile-dir at least this amount of "
                            "free space in bytes")
        parser.add_argument("--chunk-size",
                            default=constant.CHUNK_BYTES, type=int,
                            help="Outfile size generated by dump in bytes")
        parser.add_argument("--long-trx-time",
                            default=constant.LONG_TRX_TIME, type=int,
                            help="We will pause to wait for a long "
                            "transaction to finish its work before we fire "
                            "a DDL. This option defines how long a long "
                            "transaction is")
        parser.add_argument("--max-running-before-ddl",
                            default=constant.MAX_RUNNING_BEFORE_DDL, type=int,
                            help="We will pause if there're too many "
                            "concurrent threads already running before firing "
                            "any DDL command. As it may block MySQL for a "
                            "long time than it should")
        parser.add_argument("--ddl-guard-attempts",
                            default=constant.DDL_GUARD_ATTEMPTS, type=int,
                            help="For how many times we should have been "
                            "checking for running threads before we exit. "
                            "See --max-running-before-ddl for more")
        parser.add_argument("--lock-max-attempts",
                            default=constant.LOCK_MAX_ATTEMPTS, type=int,
                            help="For how many times we should have tried"
                            "to lock table for wrirte before exist. LOCK "
                            "WRITE may fail because of dead lock or lock "
                            "timeout.")
        parser.add_argument("--mysql-session-timeout",
                            default=constant.SESSION_TIMEOUT, type=int,
                            help="Session timeout for MySQL connection. "
                            "If this value is too low, we may encounter "
                            "MySQL has gone away between operations")
        parser.add_argument("--keep-tmp-table-after-exception",
                            action='store_true',
                            help="Skip cleanup if there's an exception raised "
                            "Useful when you want to investigation the root "
                            "Cause of the exception")
        parser.add_argument("--session-overrides",
                            help="Override session variables using given "
                            "string. Each override should be given in a "
                            "session_var_name=value format, and separated "
                            "by ';'")
        parser.add_argument("--skip-affected-rows-check",
                            action='store_true',
                            help="Skip affected rows check after each replay "
                            "of delta event. Use this option only if you rely "
                            "on unique_checks to perform blind writes")
        parser.add_argument("--skip-checksum",
                            action='store_true',
                            help="Skip checksum data after loading, if you're "
                            "sure about what you're doing and don't want "
                            "waste time in checksuming")
        parser.add_argument("--skip-checksum-for-modified",
                            action='store_true',
                            help="Skip checksum data for modified columns "
                            "Sometimes the format of the column will be "
                            "changed when we're changing the column type "
                            "Use this option to avoid CHECKSUM_MISMATCH "
                            "error when this happens")
        parser.add_argument("--skip-named-lock",
                            action='store_true',
                            help="Skip getting named lock for the whole OSC "
                            "duration. This named lock is a safe guard to "
                            "prevent multiple OSC processes running at the "
                            "same time. Use this option, if you really want "
                            "to run more than one OSC at the same time, and "
                            "don't care about the potential load it will "
                            "cause to the MySQL intance")
        parser.add_argument("--skip-cleanup-after-kill",
                            help="Leave the triggers and shadow table behind, "
                            "if OSC was killed.")
        parser.add_argument("--where",
                            help="Only dump rows which match this WHERE "
                            "condition. This works more like a selective "
                            "rebuild instead of a schema change")

    def setup_parser(self, parser, **kwargs):
        super(Copy, self).setup_parser(parser, **kwargs)
        self.add_file_list_parser(parser)
        self.add_engine_parser(parser)
        self.setup_optional_args(parser)

    def validate_args(self):
        if self.args.outfile_dir:
            if not os.path.exists(self.args.outfile_dir):
                raise OSCError('OUTFILE_DIR_NOT_EXIST',
                               {'dir': self.args.outfile_dir})
            if not os.path.isdir(self.args.outfile_dir):
                raise OSCError('OUTFILE_DIR_NOT_DIR',
                               {'dir': self.args.outfile_dir})

        # Ensure all the given ddl files are readable
        for filepath in self.args.ddl_file_list:
            if not util.is_file_readable(filepath):
                raise OSCError('FAILED_TO_READ_DDL_FILE',
                               {'filepath': filepath})

    def op(self, sudo=False):
        self.payload = CopyPayload(get_conn_func=self.get_conn_func,
                                   sudo=sudo,
                                   **vars(self.args))

        log.debug("Running schema change")
        self.payload.run()
