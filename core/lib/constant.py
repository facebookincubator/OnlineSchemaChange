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

PREFIX = '__osc_'
OUTFILE_TABLE = '__osc_tbl_'
OUTFILE_EXCLUDE_ID = '__osc_ex_'
OUTFILE_INCLUDE_ID = '__osc_in_'
NEW_TABLE_PREFIX = '__osc_new_'
DELTA_TABLE_PREFIX = '__osc_chg_'
RENAMED_TABLE_PREFIX = '__osc_old_'
INSERT_TRIGGER_PREFIX = '__osc_ins_'
UPDATE_TRIGGER_PREFIX = '__osc_upd_'
DELETE_TRIGGER_PREFIX = '__osc_del_'
OSC_LOCK_NAME = 'OnlineSchemaChange'

CHUNK_BYTES = 20 * 1024 * 1024
REPLAY_DEFAULT_TIMEOUT = 30
DEFAULT_BATCH_SIZE = 500
DEFAULT_REPLAY_ATTEMPT = 10
DEFAULT_RESERVED_SPACE = 1024 * 1024 * 1024
LONG_TRX_TIME = 30
MAX_RUNNING_BEFORE_DDL = 200
DDL_GUARD_ATTEMPTS = 600
LOCK_MAX_ATTEMPTS = 3
SESSION_TIMEOUT = 600
DEFAULT_REPLAY_GROUP_SIZE = 200
