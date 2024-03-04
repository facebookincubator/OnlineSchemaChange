#!/usr/bin/env python3

# pyre-ignore-all-errors
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
from osc.lib.sqlparse import CreateParser, parse_create, SchemaDiff


class SQLParserTest(BaseSQLParserTest):
    def setUp(self):
        super().setUp()
        self.parse_function = CreateParser.parse

    def test_table_name_with_backtick(self):
        sql = "Create table `foo``bar`\n" "( `column1` int )"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo`bar")
        self.assertEqual(len(tbl.column_list), 1)
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_docstore_index_str(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( a document , "
            "KEY key_name ( `a`.`b`.`c` AS STRING(10) ))"
        )
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertTrue(len(tbl.indexes), 1)
            self.assertTrue(len(tbl.indexes[0].column_list), 1)
            self.assertEqual(tbl.indexes[0].column_list[0].document_path, "`a`.`b`.`c`")
            self.assertEqual(tbl.indexes[0].column_list[0].key_type, "STRING")
            self.assertEqual(tbl.indexes[0].column_list[0].length, str(10))

    def test_docstore_index(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( a document , "
            "KEY key_name ( `a`.`b`.`c` AS INT ))"
        )
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertTrue(len(tbl.indexes), 1)
            self.assertTrue(len(tbl.indexes[0].column_list), 1)
            self.assertEqual(tbl.indexes[0].column_list[0].document_path, "`a`.`b`.`c`")
            self.assertEqual(tbl.indexes[0].column_list[0].key_type, "INT")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_simple_create_table_with_inline_pri(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 int primary )")
        sqls.append("Create table foo\n" "( column1 int primary key)")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertTrue(tbl.primary_key.is_unique)
            self.assertEqual(len(tbl.primary_key.column_list), 1)
            self.assertEqual(tbl.primary_key.column_list[0].name, "column1")
            self.assertEqual(tbl.name, "foo")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_utf8_and_utf8mb3_charset_compare(self):
        sqls = [
            (
                "Create table foo\n"
                "( column1 varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci )"
                "character set=utf8"
            ),
            (
                "Create table foo\n"
                "( column1 varchar(255)  CHARACTER SET utf8mb3  COLLATE utf8mb3_general_ci )"
                "character set=utf8"
            ),
        ]
        tbl1 = self.parse_function(sqls[0])
        tbl2 = self.parse_function(sqls[1])
        diff = SchemaDiff(tbl1, tbl2).to_sql()
        self.assertFalse(diff)


class ModelTableTestCase(BaseModelTableTestCase):
    def setUp(self):
        super().setUp()
        self.parse_function = CreateParser.parse
