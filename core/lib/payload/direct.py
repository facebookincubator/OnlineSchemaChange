#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import logging as log

import MySQLdb
from osc.lib.hook import wrap_hook

from ..error import OSCError
from .base import Payload


class DirectPayload(Payload):
    """
    This payload is usually for SQL which can be executed *online*, which
    includes CREATE and online ALTERs which are supported by MySQL.
    As it's user's choice to let a SQL being executed directly against MySQL
    or using OSC, there's no additional check to tell whether this SQL is
    a supported online ALTER or not.
    This make it possible to run non-online ALTER against MySQL directly
    when you think locking a table for certain amount of time won't be a
    big deal for your application
    """

    @wrap_hook
    def run_ddl(self, db, sql):
        log.debug("Creating table using: \n {}".format(sql))
        try:
            self.use_db(db)
            self.execute_sql(sql)
        except (MySQLdb.OperationalError, MySQLdb.ProgrammingError) as e:
            errnum, errmsg = e.args
            log.error(
                "SQL execution error: [{}] {}\n"
                "When executing: {}\n"
                "With args: {}".format(
                    errnum, errmsg, self._sql_now, self._sql_args_now
                )
            )
            raise OSCError(
                "GENERIC_MYSQL_ERROR",
                {
                    "stage": "running DDL on db '{}'".format(db),
                    "errnum": errnum,
                    "errmsg": errmsg,
                },
                mysql_err_code=errnum,
            )
