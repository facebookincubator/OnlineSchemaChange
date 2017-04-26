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

log = logging.getLogger(__name__)


class CommandBase(object):
    DESCRIPTION = ("""
    This is a default description. You should overwrite it in your sub-class.
    """)

    # args from ArgumentParser.parse_args()
    args = None

    # master ArgumentParser
    parser = None

    # subparser for this command
    subparser = None

    def __init__(self, get_conn_func=None):
        """
        Initialize the command base object, and assign the infra-dependent
        stuffs if given

        @param get_mysql_connection: Override the function to get the MySQL
        connection. If not specified here, the default connection function
        will be used, which is 'osc.lib.db.default_get_mysql_connection'.
        If you want to customize the way OSC connect to a
        MySQL instance, you can write/import one in cli.py and pass it here
        @type: function
        """
        self.get_conn_func = get_conn_func
        self.init()

    def init(self):
        """
        Called upon init, override me.
        """
        pass

    def name(self):
        """
        Return the name of this subcommand.
        """
        return self.NAME

    def description(self):
        """
        Return a one-line description of this command
        Exceptions are ignored
        """
        return self.DESCRIPTION.splitlines()[0]

    def help(self):
        """
        Return a chunk of text explaining this command
        """
        return self.DESCRIPTION

    def setup_parser(self, parser, optional_db=False,
                     require_user=False, require_password=False):
        """
        Common parser shared across all the modes
        """
        parser.add_argument("--socket",
                            help="Socket file for the mysql "
                            "connection",
                            required=True)
        parser.add_argument("--database",
                            help="Database name(s) to run the schema change",
                            nargs='+', required=(not optional_db))
        parser.add_argument("--repl-status",
                            help="Force script to run only on instances with "
                            "the replication role. ",
                            choices=['master', 'slave'])
        parser.add_argument("--mysql-user",
                            help="MySQL username to connect to the instance",
                            required=require_user)
        parser.add_argument("--mysql-password",
                            help="MySQL user password to connect to the "
                            "instance",
                            required=require_password)
        parser.add_argument("--charset",
                            help="Character set used for MySQL connection")
        parser.add_argument("--force",
                            help="Ignore non-critical errors and continue "
                            "making schema changes for all the given "
                            "databases ")

    def add_file_list_parser(self, parser):
        parser.add_argument("--ddl-file-list",
                            help="Files with CREATE statements. "
                            "Multiple files are supported as list separated "
                            "by space",
                            required=True,
                            nargs='+')

    def add_engine_parser(self, parser):
        parser.add_argument("--mysql-engine",
                            help="Make sure the table is created with only "
                            "the specified engine")

    def usage(self, *args, **kwargs):
        self.parser.error(*args, **kwargs)

    def validate_args(self):
        pass
