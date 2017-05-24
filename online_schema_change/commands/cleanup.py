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

from .base import CommandBase
from ..lib.error import OSCError
from ..lib.payload.cleanup import CleanupPayload

log = logging.getLogger(__name__)


class Cleanup(CommandBase):
    DESCRIPTION = (
        "Cleanup all the mess leftover by unclean OSC shutdown"
    )
    NAME = 'cleanup'

    def setup_parser(self, parser, **kwargs):
        super(Cleanup, self).setup_parser(parser, optional_db=True, **kwargs)
        parser.add_argument("--kill",
                            help="Kill the running OSC process if there's one",
                            action='store_true')
        parser.add_argument("--kill-only",
                            help="Exit right after killing OSC",
                            action='store_true')

    def pre_run(self):
        # Test database connection
        log.debug("Testing database connection")
        if not self.payload.init_conn():
            raise OSCError('FAILED_TO_CONNECT_DB',
                           {'user': self.payload.mysql_user,
                            'socket': self.payload.socket})

        # Fetch mysql variables from server
        if not self.payload.fetch_mysql_vars():
            raise OSCError('FAILED_TO_FETCH_MYSQL_VARS')

        # Check database existance
        if self.payload.databases:
            non_exist_dbs = self.payload.check_db_existence()
            if non_exist_dbs:
                raise OSCError('DB_NOT_EXIST',
                               {'db_list': ', '.join(non_exist_dbs)})

    def op(self, sudo=False):
        self.payload = CleanupPayload(get_conn_func=self.get_conn_func,
                                      sudo=sudo,
                                      **vars(self.args))

        log.debug("Pre-run check started")
        self.pre_run()

        log.debug("Start to run schema change")
        self.payload.cleanup_all()
