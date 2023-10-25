#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""


from dba.osc.core.lib.sqlparse.common_tests.sqlparse_lib_test import (
    BaseModelTableTestCase,
    BaseSQLParserTest,
)
from osc.lib.sqlparse import CreateParser


class SQLParserTest(BaseSQLParserTest):
    def setUp(self):
        super().setUp()
        self.parse_function = CreateParser.parse


class ModelTableTestCase(BaseModelTableTestCase):
    def setUp(self):
        super().setUp()
        self.parse_function = CreateParser.parse
