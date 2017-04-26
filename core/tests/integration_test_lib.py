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

import collections
import json
import logging
import os
import sys
import unittest

from ..lib import hook
from ..lib.payload.copy import CopyPayload
from ..lib import error

log = logging.getLogger(__name__)


def gen_test_cases(base_dir, get_conn, test_to_run=None, database='test',
                   socket='/var/lib/mysql/mysql.sock'):
    """
    Test the connection to database. If we don't have the access, don't bother
    running the tests
    """
    try:
        dbh = get_conn(socket=socket, dbname=database)
        dbh.close()
    except Exception as e:
        raise RuntimeError("Failed to connect to MySQL: %s" % str(e))

    def gen_test(test_case_dir, config, gen_conn, database):
        def test_function(self):
            hook_map = collections.defaultdict(lambda: hook.NoopHook())
            for c in config['hooks']:
                file_path = os.path.join(test_case_dir, config['hooks'][c])
                hook_map[c] = hook.SQLNewConnHook(file_path)
            for c in config.get('hooks_in_thread', {}):
                file_path = os.path.join(
                    test_case_dir, config['hooks_in_thread'][c])
                hook_map[c] = hook.SQLNewConnInThreadHook(file_path)
            ddl_list = []
            param_dict = {
                'socket': socket,
                'database': [database],
            }
            for param in config['params']:
                if param == 'ddl_file_list':
                    for filename in config['params']['ddl_file_list']:
                        ddl_list.append(os.path.join(test_case_dir, filename))
                else:
                    param_dict[param] = config['params'][param]
            try:
                payload = CopyPayload(
                    get_conn_func=get_conn,
                    ddl_file_list=ddl_list,
                    hook_map=hook_map,
                    **param_dict
                )
                payload.run()
            except error.OSCError as e:
                expect_error = config.get(
                    'expect_result', {}).get('err_key', None)
                self.assertEqual(e.err_key, expect_error)
                if expect_error == 'GENERIC_MYSQL_ERROR':
                    mysql_err_code = config.get(
                        'expect_result', {}).get('mysql_err_code', None)
                    self.assertEqual(e.mysql_err_code, mysql_err_code)

        return test_function

    func_dict = {}
    for test_dir in os.listdir(base_dir):
        test_case_dir = os.path.join(base_dir, test_dir)
        try:
            # legit test case config should be a directory
            if not os.path.isdir(test_case_dir):
                continue
            config_path = os.path.join(test_case_dir, 'config.conf')
            with open(config_path) as fh:
                contents = [
                    line for line in fh.readlines()
                    if not line.startswith('//')]
                config = json.loads("\n".join(contents))
        except Exception:
            print("Failed to decode config.json under {}"
                  .format(config_path), file=sys.stderr)
            continue
        # unittest will only detect test has its name being with "test"
        test_name = "test_{}".format(test_dir)
        if test_to_run and test_to_run not in (test_name, test_dir):
            continue
        func_dict[test_name] = gen_test(
            test_case_dir, config, get_conn, database)
    if not func_dict:
        print("No test to run", file=sys.stderr)
    return type(str('OSCTestCase'), (unittest.TestCase,), func_dict)
