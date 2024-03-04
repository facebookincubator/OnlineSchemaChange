#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import logging
import os
import sys
import warnings

import MySQLdb

from . import sql

log = logging.getLogger(__name__)


def default_get_mysql_connection(
    user_name,
    user_pass,
    socket,
    dbname="",
    timeout=60,
    connect_timeout=10,
    charset=None,
):
    """
    Default method for connection to a MySQL instance.
    You can override this behaviour by define/import in cli.py and pass it to
    Payload at instantiation time.
    The function should return a valid Connection object just as
    MySQLdb.Connect does.
    """
    connection_config = {
        "unix_socket": socket,
        "db": dbname,
        "use_unicode": True,
        "connect_timeout": connect_timeout,
    }
    if charset:
        connection_config["charset"] = charset
    if user_name:
        connection_config["user"] = user_name
    if user_pass:
        connection_config["passwd"] = user_pass

    dbh = MySQLdb.Connect(**connection_config)
    dbh.autocommit(True)
    if timeout:
        cursor = dbh.cursor()
        cursor.execute("SET SESSION WAIT_TIMEOUT = %s", (timeout,))
    return dbh


class MySQLSocketConnection:
    """
    A handy wrapper to connecting a MySQL server via a Unix domain socket.
    After a connection is established, you then can execute some basic
    operations by direct calling functions of this class.
    self.conn will contain the actual database handler.
    """

    def __init__(
        self,
        user,
        password,
        socket,
        dbname="",
        connect_timeout=10,
        connect_function=None,
        charset=None,
    ):
        self.user = user
        self.password = password
        self.db = dbname
        self.conn = None
        self.socket = socket
        self.connect_timeout = connect_timeout
        self.charset = charset
        # Cache the connection id, if the connection_id property is called.
        self._connection_id = None
        if connect_function is not None:
            self.connect_function = connect_function
        else:
            self.connect_function = default_get_mysql_connection
        self.query_header = "/* {} */".format(
            ":".join((sys.argv[0], os.path.basename(__file__)))
        )

    def connect(self):
        """Establish a connection to a database.

        If connections fail, then an exception shall likely be raised.

        @return: True if the connection was successful and False if not.
        @rtype: bool
        @raise:
        """
        self.conn = self.connect_function(
            self.user,
            self.password,
            self.socket,
            self.db,
            connect_timeout=self.connect_timeout,
            charset=self.charset,
        )

    def disconnect(self):
        """Close an existing open connection to a MySQL server."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def use(self, database_name):
        """Set context to a given database.
        @param database_name: A database that exists.
        @type database_name: str | unicode
        """
        self.conn.query("USE `{0}`".format(database_name))

    def set_no_binlog(self):
        """
        Disable session binlog events. As we run the schema change separately
        on instance, we usually don't want the changes to be populated through
        replication.
        """
        self.conn.query("SET SESSION SQL_LOG_BIN=0;")

    def set_binlog(self):
        """
        Enable session binlog events. Providing an option to run schema change
        via replication when applicable.
        """
        self.conn.query("SET SESSION SQL_LOG_BIN=1;")

    def affected_rows(self):
        """
        Return the number of affected rows of the last query ran in this
        connection
        """
        return self.conn.affected_rows

    def query(self, sql, args=None):
        """
        Run the sql query, and return the result set
        """
        cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("%s %s" % (self.query_header, sql), args)
        return cursor.fetchall()

    def query_array(self, sql, args=None):
        """
        Run the sql query, and return the result set
        """
        cursor = self.conn.cursor(MySQLdb.cursors.Cursor)
        cursor.execute("%s %s" % (self.query_header, sql), args)
        return cursor.fetchall()

    def execute(self, sql, args=None):
        """
        Execute the given sql against current open connection
        without caring about the result output
        """
        # Turning MySQLdb.Warning into exception, so that we can catch it
        # and maintain the same log output format
        with warnings.catch_warnings():
            warnings.filterwarnings("error", category=MySQLdb.Warning)
            try:
                cursor = self.conn.cursor()
                cursor.execute("%s %s" % (self.query_header, sql), args)
            except Warning as db_warning:
                log.warning(
                    "MySQL warning: {}, when executing sql: {}, args: {}".format(
                        db_warning, sql, args
                    )
                )
            return cursor.rowcount

    def get_running_queries(self):
        """
        Get a list of running queries. A wrapper of a single query to make it
        easier for writing unittest
        """
        return self.query(sql.show_processlist)

    def kill_query_by_id(self, id):
        """
        Kill query with given query id. A wrapper of a single query to make it
        easier for writing unittest
        """
        self.execute(sql.kill_proc, (id,))

    def ping(self):
        self.conn.ping()

    def close(self):
        self.conn.close()
