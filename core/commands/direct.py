#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import logging

from ..lib import util
from ..lib.error import OSCError
from ..lib.payload.direct import DirectPayload
from .base import CommandBase

log = logging.getLogger(__name__)


class Direct(CommandBase):
    DESCRIPTION = (
        "Direct mode. In this mode, all the schema change SQLs will be "
        "executed directly against MySQL server. \n It has the same "
        "behaviour as running SQL directly through mysql client."
    )
    NAME = "direct"

    def setup_parser(self, parser, **kwargs):
        super(Direct, self).setup_parser(parser, **kwargs)
        self.add_file_list_parser(parser)
        self.add_engine_parser(parser)
        parser.add_argument(
            "--standardize",
            action="store_true",
            help="Standardize SQL in files before executing. "
            "Keywords will be capitalized, and column "
            "properties will be re-arranged according to the "
            "Mysql manual",
        )

    def pre_run(self):
        # Ensure all the given ddl files are readable
        for filepath in self.args.ddl_file_list:
            if not util.is_file_readable(filepath):
                raise OSCError(
                    OSCError.Errors.FAILED_TO_READ_DDL_FILE, {"filepath": filepath}
                )
        self.payload.ddl_file_list = self.args.ddl_file_list

        # Test database connection
        log.debug("Testing database connection")
        if not self.payload.init_conn():
            raise OSCError(
                OSCError.Errors.FAILED_TO_CONNECT_DB,
                {"user": self.payload.mysql_user, "socket": self.payload.socket},
            )

        # Test whether the replication role matches
        log.debug("Verifying replication role")
        if self.args.repl_status:
            if not self.payload.check_replication_type():
                raise OSCError(
                    OSCError.Errors.REPL_ROLE_MISMATCH,
                    {"given_role": self.payload.repl_status},
                )

        # Fetch mysql variables from server
        if not self.payload.fetch_mysql_vars():
            raise OSCError(OSCError.Errors.FAILED_TO_FETCH_MYSQL_VARS)

        # Check database existence
        non_exist_dbs = self.payload.check_db_existence()
        if non_exist_dbs:
            raise OSCError(
                OSCError.Errors.DB_NOT_EXIST, {"db_list": ", ".join(non_exist_dbs)}
            )

    def op(self, *args, **kwargs):
        self.payload = DirectPayload(
            get_conn_func=self.get_conn_func, **vars(self.args)
        )

        log.debug("Pre-run check started")
        self.pre_run()

        log.debug("Start to run schema change")
        self.payload.run()
