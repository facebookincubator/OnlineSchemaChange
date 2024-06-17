#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

PREFIX = "__osc_"
OUTFILE_TABLE = "__osc_tbl_"
OUTFILE_EXCLUDE_ID = "__osc_ex_"
OUTFILE_INCLUDE_ID = "__osc_in_"
NEW_TABLE_PREFIX = "__osc_new_"
DELTA_TABLE_PREFIX = "__osc_chg_"
RENAMED_TABLE_PREFIX = "__osc_old_"
INSERT_TRIGGER_PREFIX = "__osc_ins_"
UPDATE_TRIGGER_PREFIX = "__osc_upd_"
DELETE_TRIGGER_PREFIX = "__osc_del_"

# tables with 64 character length names need a generic place-holder name
GENERIC_TABLE_NAME = "online_schema_change_temp_tbl"

# Special prefixes for tables that have longer table names
SHORT_NEW_TABLE_PREFIX = "n!"
SHORT_DELTA_TABLE_PREFIX = "c!"
SHORT_RENAMED_TABLE_PREFIX = "o!"
SHORT_INSERT_TRIGGER_PREFIX = "i!"
SHORT_UPDATE_TRIGGER_PREFIX = "u!"
SHORT_DELETE_TRIGGER_PREFIX = "d!"

OSC_LOCK_NAME = "OnlineSchemaChange"

CHUNK_BYTES = 2 * 1024 * 1024
REPLAY_DEFAULT_TIMEOUT = 5  # replay until we can finish in 5 seconds
DEFAULT_BATCH_SIZE = 500
DEFAULT_REPLAY_ATTEMPT = 15
DEFAULT_RESERVED_SPACE_PERCENT = 1
LONG_TRX_TIME = 30
MAX_RUNNING_BEFORE_DDL = 200
DDL_GUARD_ATTEMPTS = 600
LOCK_MAX_ATTEMPTS = 3
LOCK_MAX_WAIT_BEFORE_KILL_SECONDS = 0.5
SESSION_TIMEOUT = 604800  # 7 days, some tables are large
DEFAULT_REPLAY_GROUP_SIZE = 200
PK_COVERAGE_SIZE_THRESHOLD = 500 * 1024 * 1024
MAX_WAIT_FOR_SLOW_QUERY = 100
MAX_TABLE_LENGTH = 64
MAX_REPLAY_BATCH_SIZE = 500000
MAX_REPLAY_CHANGES = 2146483647
WSENV_CHUNK_BYTES = 64 * 1024 * 1024
CHECKSUM_CHUNK_BYTES = 64 * 1024 * 1024
GC_COLLECT_TIME_INTERVAL = 120  # 2 minutes
