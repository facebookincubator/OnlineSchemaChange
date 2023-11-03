#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import logging
import unittest

from osc.lib.sqlparse import ParseError

log = logging.getLogger(__name__)


class BaseSQLParserTest(unittest.TestCase):
    def setUp(self):
        self.parse_function = None

    def skipTestIfBaseClass(self, reason):
        if not self.parse_function:
            self.skipTest(reason)

    def test_simple_create_table(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 int )"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(len(tbl.column_list), 1)
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_json_column(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 json )"
        tbl = self.parse_function(sql)
        self.assertTrue(tbl.has_80_features)
        self.assertEqual(len(tbl.column_list), 1)
        self.assertEqual(tbl.column_list[0].column_type, "JSON")
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_desc_index(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = """
        CREATE TABLE `test_table_1` (
        `id` bigint unsigned NOT NULL AUTO_INCREMENT,
        `a` int DEFAULT NULL,
        `t` char(1) NOT NULL DEFAULT 't',
        PRIMARY KEY (`id` DESC),
        KEY `t_index` (`t`),
        KEY `a_index` (`a` ASC),
        KEY `a_index_desc` (`a` DESC),
        KEY `a_t_composite_index` (`a` ASC, `t` DESC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        tbl = self.parse_function(sql)
        self.assertTrue(tbl.has_80_features)
        for idx in tbl.indexes:
            if idx.name == "t_index":
                self.assertEqual(len(idx.column_list), 1)
                self.assertEqual(idx.column_list[0].name, "t")
                self.assertEqual(idx.column_list[0].order, "ASC")
            elif idx.name == "a_index":
                self.assertEqual(len(idx.column_list), 1)
                self.assertEqual(idx.column_list[0].name, "a")
                self.assertEqual(idx.column_list[0].order, "ASC")
            elif idx.name == "a_index_desc":
                self.assertEqual(len(idx.column_list), 1)
                self.assertEqual(idx.column_list[0].name, "a")
                self.assertEqual(idx.column_list[0].order, "DESC")
            elif idx.name == "a_t_composite_index":
                self.assertEqual(len(idx.column_list), 2)
                for col in idx.column_list:
                    if col.name == "a":
                        self.assertEqual(col.order, "ASC")
                    elif col.name == "t":
                        self.assertEqual(col.order, "DESC")
                    else:
                        raise Exception("Wrong column name")
            else:
                raise Exception("Wrong index name")
        self.assertEqual(len(tbl.primary_key.column_list), 1)
        self.assertEqual(tbl.primary_key.column_list[0].order, "DESC")
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_table_name_quoted_with_backtick(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table `foo`\n" "( column1 int )"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(len(tbl.column_list), 1)
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_column_name_quoted_with_backtick(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( `column1` int )"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(len(tbl.column_list), 1)
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_simple_with_all_supported_int_type(self):
        self.skipTestIfBaseClass("Need to implement base class")
        supported_type = ["int", "tinyint", "bigint", "mediumint", "smallint"]
        for col_type in supported_type:
            for unsigned in ["unsigned", ""]:
                sql = "Create table foo\n" "( column1 {} {})".format(col_type, unsigned)
                tbl = self.parse_function(sql)
                self.assertTrue(tbl.primary_key.is_unique)
                self.assertEqual(len(tbl.column_list), 1)
                self.assertEqual(tbl.column_list[0].name, "column1")
                self.assertEqual(tbl.column_list[0].column_type, col_type.upper())
                self.assertEqual(tbl.name, "foo")
                self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_table_comment(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 int )" "comment='table comment'"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(tbl.comment, "'table comment'")
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_table_charset(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = [
            ("Create table foo\n" "( column1 int )" "character set=utf8"),
            ("Create table foo\n" "( column1 int )" "default character set=utf8"),
        ]
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.name, "foo")
            self.assertEqual(tbl.charset, "utf8")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_bare_column_collate(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 varchar(10) collate latin1_bin )"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(tbl.column_list[0].collate, "latin1_bin")
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_bare_column_charset(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 varchar(10) character set latin1 )"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(tbl.column_list[0].charset, "latin1")
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_table_collate(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = [
            ("Create table foo\n" "( column1 int )" "collate='utf8_bin'"),
            ("Create table foo\n" "( column1 int )" "default collate='utf8_bin'"),
        ]
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.name, "foo")
            self.assertEqual(tbl.collate, "utf8_bin")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_table_key_block_size(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 int )" "key_block_size=16"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(tbl.key_block_size, 16)
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_engine(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 int )" "engine=Innodb"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(tbl.engine, "INNODB")
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_table_incre(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "Create table foo\n" "( column1 int )" "auto_increment=123"
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.name, "foo")
        self.assertEqual(tbl.auto_increment, 123)
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_row_format(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = [("Create table foo\n" "( column1 int )" "row_format=compressed")]
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.name, "foo")
            self.assertEqual(tbl.row_format, "COMPRESSED")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_simple_with_all_supported_int_type_and_length(self):
        self.skipTestIfBaseClass("Need to implement base class")
        supported_type = ["int", "tinyint", "bigint", "mediumint", "smallint"]
        for col_type in supported_type:
            for unsigned in ["unsigned", ""]:
                sql = "Create table foo\n" "( column1 {}(10) {})".format(
                    col_type, unsigned
                )
                tbl = self.parse_function(sql)
                self.assertTrue(tbl.primary_key.is_unique)
                self.assertEqual(len(tbl.column_list), 1)
                self.assertEqual(tbl.column_list[0].name, "column1")
                self.assertEqual(tbl.column_list[0].length, str(10))
                self.assertEqual(tbl.column_list[0].column_type, col_type.upper())
                self.assertEqual(tbl.name, "foo")
                self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_trailing_pri(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 int , " "primary key (column1))")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertTrue(tbl.primary_key.is_unique)
            self.assertEqual(len(tbl.primary_key.column_list), 1)
            self.assertEqual(tbl.primary_key.column_list[0].name, "column1")
            self.assertEqual(tbl.name, "foo")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_multiple_tailing_index(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int, \n"
            "column2 varchar(10),\n"
            "key `index_name1` (column1, column2(5) ) comment 'a comment',\n"
            " UNIQUE key `index_name2` (column1 )\n"
            ")"
        )
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(len(tbl.indexes), 2)
            self.assertEqual(len(tbl.indexes[0].column_list), 2)
            self.assertEqual(len(tbl.indexes[1].column_list), 1)
            self.assertFalse(tbl.indexes[0].is_unique)
            self.assertEqual(tbl.indexes[0].column_list[0].name, "column1")
            self.assertEqual(tbl.indexes[0].column_list[1].name, "column2")
            self.assertEqual(tbl.indexes[0].comment, "'a comment'")
            self.assertEqual(tbl.indexes[1].column_list[0].name, "column1")
            self.assertTrue(tbl.indexes[1].is_unique)
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_fulltext_index(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int, \n"
            "column2 varchar(10),\n"
            " FULLTEXT key `index_name` (column1 )\n"
            ")"
        )
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(len(tbl.indexes), 1)
            self.assertEqual(len(tbl.indexes[0].column_list), 1)
            self.assertFalse(tbl.indexes[0].is_unique)
            self.assertEqual(tbl.indexes[0].name, "index_name")
            self.assertEqual(tbl.indexes[0].key_type, "FULLTEXT")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_default_value_int(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 int default 0) ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].default, "0")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_default_value_double(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n ( "
            "column1 double default 0.0, "
            "column2 double default 0, "
            "column3 double default '0'"
            ") "
        )
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].default, "0.0")
            self.assertEqual(tbl.column_list[1].default, "0")
            self.assertEqual(tbl.column_list[2].default, "'0'")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_default_value_string(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 int default '0') ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].default, "'0'")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_default_value_empty_string(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 char(1) default '') ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].default, "''")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_nullable(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 int null) ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertTrue(tbl.column_list[0].nullable)
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_explicit_nullable(self):
        self.skipTestIfBaseClass("Need to implement base class")
        # explicitly specify nullable and implicitly should be identical
        left = "Create table foo\n" "( column1 int null) "
        right = "Create table foo\n" "( column1 int) "
        left_obj = self.parse_function(left)
        right_obj = self.parse_function(right)
        self.assertEqual(left_obj, right_obj)

    def test_not_nullable(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 int not null) ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertFalse(tbl.column_list[0].nullable)
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_col_type_timestamp(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n" "( column1 timestamp default current_timestamp) "
        )
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].default, "CURRENT_TIMESTAMP")

    def test_col_type_timestamp_on_update(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 timestamp(10) default current_timestamp "
            "on update current_timestamp) "
        )
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].default, "CURRENT_TIMESTAMP")
            self.assertEqual(
                tbl.column_list[0].on_update_current_timestamp, "CURRENT_TIMESTAMP"
            )

    def test_explicit_timestamp_default_for_bare_timestamp(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = "Create table foo\n" "( column1 timestamp(10) ) "
        right = (
            "Create table foo\n"
            "( column1 timestamp(10) NOT NULL default current_timestamp "
            "on update current_timestamp) "
        )
        self.assertEqual(self.parse_function(left), self.parse_function(right))

    def test_explicit_timestamp_default_for_not_null(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = "Create table foo\n" "( column1 timestamp(10) NOT NULL ) "
        right = (
            "Create table foo\n"
            "( column1 timestamp(10) NOT NULL default current_timestamp "
            "on update current_timestamp) "
        )
        self.assertEqual(self.parse_function(left), self.parse_function(right))

    def test_no_accidentally_explicit_timestamp_default_for(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = "Create table foo\n" "( column1 timestamp(10) NULL ) "
        right = (
            "Create table foo\n"
            "( column1 timestamp(10) NOT NULL default current_timestamp "
            "on update current_timestamp) "
        )
        self.assertNotEqual(self.parse_function(left), self.parse_function(right))

    def test_col_collate(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 varchar(10) collate utf8_bin) ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].collate, "utf8_bin")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_col_charset(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 varchar(10) character set utf8) ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].charset, "utf8")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_col_comment(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 int comment 'column comment') ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].comment, "'column comment'")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_index_use_btree(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "Create table foo\n"
            "(id int primary key, column1 int, "
            "key m_idx (column1) USING BTREE "
            ") "
        )
        tbl = self.parse_function(sql)
        self.assertEqual(tbl.indexes[0].using.upper(), "BTREE")
        self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_default_value_bit(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo\n" "( column1 bit default b'0') ")
        for sql in sqls:
            tbl = self.parse_function(sql)
            self.assertEqual(tbl.column_list[0].default, "'0'")
            self.assertTrue(tbl.column_list[0].is_default_bit, "'0'")
            self.assertEqual(tbl, self.parse_function(tbl.to_sql()))

    def test_foreign_key(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "Create table foo\n"
            "( column1 int primary key, "
            "foreign key (column1) references table2 (column1))"
        )
        # Force users to create fks with the constraint
        with self.assertRaises(ParseError):
            self.parse_function(sql)
        sql = (
            "Create table foo\n"
            "( column1 int primary key, "
            "constraint `key_with_name_1` foreign key (column1) "
            "references table2 (column1), "
            "constraint `key_with_name_2` foreign key (column2) "
            "references table2 (column2))"
        )
        sql_obj = self.parse_function(sql)
        self.assertTrue(sql_obj.constraint != "")
        self.assertTrue(sql_obj.fk_constraint != {})
        sql = "Create table foo\n( column1 int primary key) "
        sql_obj = self.parse_function(sql)
        self.assertTrue(sql_obj.constraint == "")
        self.assertTrue(sql_obj.fk_constraint == {})

    def test_multiple_primary(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n" "( column1 int primary key, " "primary key (column1))"
        )
        for sql in sqls:
            with self.assertRaises(ParseError):
                self.parse_function(sql)

    def test_to_sql_consistency(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (column1) ,"
            "key `aname` (column1, column2(19)) )"
        )
        for sql in sqls:
            tbl = self.parse_function(sql).to_sql()
            str_after_parse = self.parse_function(tbl).to_sql()
            self.assertEqual(tbl, str_after_parse)

    def test_boolean_and_bool(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sqls = []
        sqls.append("Create table foo(column1 bool)")
        sqls.append("Create table foo(column1 boolean)")
        for sql in sqls:
            # should not raise
            self.parse_function(sql)

    def test_inequallity_in_index_col_length(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foobar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(101) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        right = (
            "Create table `foobar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertNotEqual(left.indexes, right.indexes)

    def test_inequallity_in_col_type(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        right = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 int(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertNotEqual(left.column_list, right.column_list)

    def test_inequallity_in_col_default(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foobar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abcd',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        right = (
            "Create table `foobar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertNotEqual(left.column_list, right.column_list)

    def test_inequallity_in_col_name(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foobar`\n"
            "( `column``1` bit default b'0',"
            " column3 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        right = (
            "Create table `foobar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertNotEqual(left.column_list, right.column_list)

    def test_inequallity_in_index_length(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        right = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertNotEqual(left.indexes, right.indexes)

    def test_inequallity_in_set(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 set('a','b'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        right = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 set('a','b','c'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertNotEqual(left, right)

    def test_inequallity_in_enum(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 enum('a','b'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(20)) )"
        )
        right = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 enum('a','b','c'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertNotEqual(left, right)

    def test_identical_equallity(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = right = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertEqual(left.indexes, right.indexes)
        self.assertEqual(left.column_list, right.column_list)
        self.assertEqual(left, right)
        self.assertFalse(left != right)

    def test_implicit_default_for_nullable(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = "Create table `foobar`\n" "( `column1` int ," "PRIMARY key (`column1`))"
        right = (
            "Create table `foobar`\n"
            "( `column1` int default null,"
            "PRIMARY key (`column1`))"
        )
        right_default = (
            "Create table `foobar`\n"
            "( `column1` int default 123,"
            "PRIMARY key (`column1`))"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertEqual(left, right)

        right = self.parse_function(right_default)
        self.assertNotEqual(left, right)

    def test_implicit_quote_for_default(self):
        # numeric defaults will automatically be quoted by MySQL, so they
        # are the same
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foobar`\n"
            "( `column1` int default 0,"
            "PRIMARY key (`column1`))"
        )
        right = (
            "Create table `foobar`\n"
            "( `column1` int default '0',"
            "PRIMARY key (`column1`))"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertEqual(left, right)

    def test_implicit_quote_for_not_nulldefault(self):
        # numeric defaults will automatically be quoted by MySQL, so they
        # are the same
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foobar`\n"
            "( `column1` int not null default 0,"
            "PRIMARY key (`column1`))"
        )
        right = (
            "Create table `foobar`\n"
            "( `column1` int not null default '0',"
            "PRIMARY key (`column1`))"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertEqual(left, right)


class BaseModelTableTestCase(unittest.TestCase):
    def setUp(self):
        self.parse_function = None

    def skipTestIfBaseClass(self, reason):
        if not self.parse_function:
            self.skipTest(reason)

    def test_identical_table_checksum(self):
        self.skipTestIfBaseClass("Need to implement base class")
        left = right = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertEqual(left.checksum, right.checksum)

    def test_different_cases_in_type(self):
        """
        Upper/Lower case in column type shouldn't affect the value of checksum
        """
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        right = (
            "Create table `foo``bar`\n"
            "( `column``1` BIT default b'0',"
            " column2 VARCHAR(100) default 'abc',"
            " column3 TIMESTAMP on update current_timestamp,"
            " column4 ENUM('a','b'),"
            " column5 SET('a','b'),"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        left = self.parse_function(left)
        right = self.parse_function(right)
        self.assertEqual(left.checksum, right.checksum)

    def test_tables_with_different_idx_seq_equal(self):
        """
        index sequence shouldn't affect the fact that two tables are identical
        """
        self.skipTestIfBaseClass("Need to implement base class")
        left = (
            "Create table `foobar` "
            "( `column1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "key (`column1`) ,"
            "key key2 (`column2`) )"
        )
        right = (
            "Create table `foobar` "
            "( `column1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "key `key2` (`column2`) ,"
            "key (`column1`) )"
        )
        left_obj = self.parse_function(left)
        right_obj = self.parse_function(right)
        self.assertEqual(left_obj, right_obj)

    def test_droppable_index(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "Create table `foo``bar`\n"
            "( "
            " `auto_inc` int auto_increment,"
            " `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "PRIMARY key (`column``1`) ,"
            "key `idx_name` (column1, column2(19)) ,"
            "unique key `idx_name2` (column1),"
            "key (auto_inc))"
        )
        log.info(sql)
        sql_obj = self.parse_function(sql)
        # both `idx_name` `idx_name2` are droppable
        droppable_indexes = sql_obj.droppable_indexes(keep_unique_key=False)
        self.assertEqual(len(droppable_indexes), 2)
        for idx in droppable_indexes:
            self.assertTrue(idx.name in ["idx_name", "idx_name2"])

        # only `idx_name` is droppable
        droppable_indexes = sql_obj.droppable_indexes(keep_unique_key=True)
        self.assertEqual(len(droppable_indexes), 1)
        for idx in droppable_indexes:
            self.assertEqual(idx.name, "idx_name")

    def test_is_myrocks_ttl_table(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "Create table `foo`\n"
            "( "
            " `id` int auto_increment,"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (id) COMMENT 'this is pk' "
            ") ENGINE=ROCKSDB COMMENT='ttl_duration=123;' "
        )
        sql_obj = self.parse_function(sql)
        self.assertTrue(sql_obj.is_myrocks_ttl_table)

    def test_is_myrocks_ttl_table_partition_ttl(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "Create table `foo`\n"
            "( "
            " `id` int auto_increment,"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (id) COMMENT 'P1_cfname=p1_v1' "
            ") ENGINE=ROCKSDB COMMENT='P1_ttl_duration=123;' "
        )
        sql_obj = self.parse_function(sql)
        self.assertTrue(sql_obj.is_myrocks_ttl_table)

    def test_is_myrocks_ttl_table_not_ttl(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "Create table `foo`\n"
            "( "
            " `id` int auto_increment,"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (id) COMMENT 'P1_cfname=p1_v1' "
            ") ENGINE=ROCKSDB COMMENT='some other comment' "
        )
        sql_obj = self.parse_function(sql)
        self.assertFalse(sql_obj.is_myrocks_ttl_table)

    def test_comma_separated_attrs(self):
        # Schema and attrs are the same except that in sql2, the
        # attributes are comma separated
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ") charset=utf8 engine=INNODB comment='hello, world'"
        )

        sql2 = (
            "Create table foo ("
            "column1 int NOT NULL AUTO_INCREMENT, "
            "column2 varchar(10) default '', "
            "column3 int default 0 "
            ") charset=utf8 , engine=INNODB, comment='hello, world'"
        )

        # Parsed objects must be identical
        obj1 = self.parse_function(sql1)
        obj2 = self.parse_function(sql2)
        self.assertEqual(obj1, obj2)

        # Comment attrib should be the same
        attr1 = obj1.comment or None
        attr2 = obj2.comment or None
        self.assertIsNotNone(attr1)
        self.assertIsNotNone(attr2)
        self.assertEqual(attr1, "'hello, world'")
        self.assertEqual(attr1, attr2)

    def test_ignore_ints_display_width(self):
        self.skipTestIfBaseClass("Need to implement base class")
        sql1 = (
            "create table a ("
            "column1 int(11), "
            "column2 bigint(20), "
            "column4 tinyint(2), "
            "column3 smallint(5), "
            "column5 mediumint(15)"
            ")"
        )
        sql2 = (
            "create table a ("
            "column1 int, "
            "column2 bigint, "
            "column4 tinyint, "
            "column3 smallint, "
            "column5 mediumint"
            ")"
        )
        self.assertEqual(self.parse_function(sql1), self.parse_function(sql2))

    def test_partitions_basic(self):
        # See partitions_parser_test.py for elaborate tests, this
        # just tests that CreateParser can invoke parse_partitions/partition_to_model

        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "CREATE TABLE `t9` ("
            "`id` int(11) NOT NULL,"
            "`blob` varbinary(40000) DEFAULT NULL,"
            "`identity` varbinary(256) DEFAULT NULL,"
            "`object_id` varbinary(256) DEFAULT NULL,"
            "`created_at` bigint(20) DEFAULT NULL,"
            "PRIMARY KEY (`id`),"
            "KEY `identity` (`identity`),"
            "KEY `object_id` (`object_id`),"
            "KEY `created_at` (`created_at`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1"
            "/*!50100 PARTITION BY RANGE (id) ("
            "PARTITION p0 VALUES LESS THAN (6) ENGINE = 'innodb' COMMENT 'whatever',"
            "PARTITION p1 VALUES LESS THAN (11),"
            "PARTITION p2 VALUES LESS THAN (16),"
            "PARTITION p3 VALUES LESS THAN (21),"
            "PARTITION p4 VALUES LESS THAN maxvalue"
            ") */"
        )

        schema_obj = self.parse_function(sql)
        self.assertIsNotNone(schema_obj.partition)
        self.assertIsNotNone(schema_obj.partition_config)
        pc = schema_obj.partition_config  # models.PartitionConfig
        self.assertEqual("RANGE", pc.get_type())
        self.assertEqual(5, pc.get_num_parts())
        self.assertEqual(["id"], pc.get_fields_or_expr())

    def test_partitions_failure(self):
        # Table schema is OK but partitions config is broken
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "CREATE TABLE `t9` ("
            "`id` int(11) NOT NULL,"
            "`blob` varbinary(40000) DEFAULT NULL,"
            "`identity` varbinary(256) DEFAULT NULL,"
            "`object_id` varbinary(256) DEFAULT NULL,"
            "`created_at` bigint(20) DEFAULT NULL,"
            "PRIMARY KEY (`id`),"
            "KEY `identity` (`identity`),"
            "KEY `object_id` (`object_id`),"
            "KEY `created_at` (`created_at`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1"
            # Note: No partitions defs while RANGE needs them.
            "/*!50100 PARTITION BY RANGE (id) */"
        )
        with self.assertRaises(ParseError):
            _ = self.parse_function(sql)

    def test_partitions_notpresent(self):
        # No partition config in DDL
        self.skipTestIfBaseClass("Need to implement base class")
        sql = (
            "CREATE TABLE `t9` ("
            "`id` int(11) NOT NULL,"
            "`blob` varbinary(40000) DEFAULT NULL,"
            "`identity` varbinary(256) DEFAULT NULL,"
            "`object_id` varbinary(256) DEFAULT NULL,"
            "`created_at` bigint(20) DEFAULT NULL,"
            "PRIMARY KEY (`id`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1"
        )

        schema_obj = self.parse_function(sql)
        self.assertIsNone(schema_obj.partition)
        self.assertIsNone(schema_obj.partition_config)

    def test_utf8_alias_charset_equality(self):
        self.skipTestIfBaseClass("Need to implement base class")
        t1 = "CREATE TABLE `t1`(s1 CHAR(1)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
        t2 = "CREATE TABLE `t1`(s1 CHAR(1)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"
        obj1 = self.parse_function(t1)
        obj2 = self.parse_function(t2)
        self.assertTrue(obj1 == obj2)

    def test_single_ddl(self):
        """
        Verify that only a single CREATE TABLE statement is permitted.
        """
        self.skipTestIfBaseClass("Need to implement base class")
        sql = "CREATE TABLE `t1`(s1 CHAR(1)); CREATE TABLE t2(foo int);"
        with self.assertRaises(ParseError):
            _ = self.parse_function(sql)
