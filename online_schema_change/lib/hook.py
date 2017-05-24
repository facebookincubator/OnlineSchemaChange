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

import codecs
import functools
import logging
import time

from .error import OSCError
from threading import Thread

log = logging.getLogger(__name__)


def wrap_hook(func):
    """
    Decorator for wrapping hooks around payload functions.
    decorate function with @wrap_hook so that two hooks point will
    be automatically wrapped around the execution.
    For example:
    @wrap_hook
    def foo(self):
        pass
    will register both 'before_foo' and 'after_foo', which will be invoked
    before and after the foo function being executed.
    """
    @functools.wraps(func)
    def func_with_hook(self, *args, **kwargs):
        self.execute_hook('before_{}'.format(func.__name__))
        result = func(self, *args, **kwargs)
        self.execute_hook('after_{}'.format(func.__name__))
        return result
    return func_with_hook


class HookBase(object):
    """
    Base hook, cannot be used directly
    """
    def __init__(self, critical=False, **kwargs):
        self.critical = critical

    def execute(self, payload):
        try:
            self._execute(payload)
            log.debug("Hook excution finished")
        except Exception as e:
            if self.critical:
                raise
            else:
                log.exception("Hook execution error: {}".format(e))

    def _execute(self, payload):
        raise NotImplementedError("_execute function in Hook not implemented")


class NoopHook(HookBase):
    """
    None-op hook, this is the default hook if we don't specify an override for
    certain hook point
    """
    def _execute(self, payload):
        log.debug("Noop hook, doing nothing here")


class SQLHook(HookBase):
    """
    Hook for executing SQLs inside sql_file_path
    """
    def __init__(self, sql_file_path='', *args, **kwargs):
        super(SQLHook, self).__init__(*args, **kwargs)
        self.file_path = sql_file_path
        self._dbh = None
        self._is_select = None
        self._sqls = []
        self._expected_lines = []
        self.read_sqls()

    def read_sqls(self):
        log.debug("Reading {}".format(self.file_path))
        with codecs.open(self.file_path, "r", "utf-8") as fh:
            current_sql = ''
            for line in fh:
                # ignore sql comments
                if line.startswith('--'):
                    continue
                # ignore empty line
                if not line.strip():
                    continue
                if self._is_select is None:
                    if line.startswith('SELECT'):
                        # The first line of expected result is always SELECT
                        # statement
                        self._is_select = True
                        self.critical = True
                        self._sqls.append(line)
                        continue
                    else:
                        self._is_select = False
                if self._is_select:
                    self._expected_lines.append(line.strip())
                else:
                    current_sql += line
                    if line.endswith(';\n'):
                        self._sqls.append(current_sql)
                        current_sql = ''
        log.debug(self._sqls)

    def execute_sqls(self):
        """
        Execute the given sql against MySQL without caring about the result
        output
        """
        # If the first line start with 'SELECT' then it's an assertion. We
        # should check the result set we get from MySQL against the rows
        # written as the rest of the SQL file
        if self._is_select:
            result = self._dbh.query_array(self._sqls[0])
            if len(result) != len(self._expected_lines):
                raise OSCError(
                    'ASSERTION_ERROR',
                    {
                        'expected': "{} lines of result set".format(
                            len(self._expected_lines)),
                        'got': "{} lines of result set".format(len(result))
                    })

            for idx, expected_row in enumerate(self._expected_lines):
                got_line = "\t".join([str(col) for col in result[idx]])
                if got_line != expected_row:
                    raise OSCError(
                        'ASSERTION_ERROR',
                        {'expected': expected_row, 'got': got_line})
        else:
            for sql in self._sqls:
                log.debug(
                    "Running the following SQL on MySQL: {} ".format(sql))
                self._dbh.execute(sql)

    def _execute(self, payload):
        self._dbh = payload.conn
        log.info("Running sql file: {} for {}"
                 .format(self.file_path, payload.socket))


class SQLNewConnHook(SQLHook):
    """
    Hook for execute SQLs inside a file using a separate connection to
    database. This is useful when you don't want to reuse the same connection
    as the one used for OSC operation, so that it can has different sql_mode,
    session setting, etc
    """
    def _execute(self, payload):
        self._dbh = payload.get_conn(payload.current_db)
        log.info("Running sql file: {} for {}"
                 .format(self.file_path, payload.socket))
        self.execute_sqls()
        self._dbh.close()


class SQLNewConnInThreadHook(SQLHook):
    """
    Hook for execute SQLs inside a file using a separate database connection
    and inside a separate thread.
    This is useful when you want to run a slow SQL in the trigger, and don't
    want to block the main OSC logic from running
    """
    def _execute(self, payload):
        thd = Thread(target=self.execute_sqls)
        self._dbh = payload.get_conn(payload.current_db)
        log.info("Running sql file: {} for {}"
                 .format(self.file_path, payload.socket))
        thd.start()
        # Wait for a while to make sure the SQL has started running before we
        # proceed
        time.sleep(1)
