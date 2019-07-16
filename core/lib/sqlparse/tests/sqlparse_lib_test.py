"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
from ...sqlparse import parse_create, ParseError


class SQLParserTest(unittest.TestCase):

    def test_simple_create_table(self):
        sql = (
            "Create table foo\n"
            "( column1 int )"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(len(tbl.column_list), 1)

    def test_table_name_quoted_with_backtick(self):
        sql = (
            "Create table `foo`\n"
            "( column1 int )"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(len(tbl.column_list), 1)

    def test_column_name_quoted_with_backtick(self):
        sql = (
            "Create table foo\n"
            "( `column1` int )"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(len(tbl.column_list), 1)

    def test_table_name_with_backtick(self):
        sql = (
            "Create table `foo``bar`\n"
            "( `column1` int )"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo`bar')
        self.assertEqual(len(tbl.column_list), 1)

    def test_simple_with_all_supported_int_type(self):
        supported_type = [
            'int', 'tinyint', 'bigint', 'mediumint', 'smallint'
        ]
        for col_type in supported_type:
            for unsigned in ['unsigned', '']:
                sql = (
                    "Create table foo\n"
                    "( column1 {} {})".format(col_type, unsigned)
                )
                tbl = parse_create(sql)
                self.assertTrue(tbl.primary_key.is_unique)
                self.assertEqual(len(tbl.column_list), 1)
                self.assertEqual(tbl.column_list[0].name, 'column1')
                self.assertEqual(
                    tbl.column_list[0].column_type, col_type.upper())
                self.assertEqual(tbl.name, 'foo')

    def test_table_comment(self):
        sql = (
            "Create table foo\n"
            "( column1 int )"
            "comment='table comment'"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(tbl.comment, "'table comment'")

    def test_table_charset(self):
        sqls = [
            (
                "Create table foo\n"
                "( column1 int )"
                "character set=utf8"
            ), (
                "Create table foo\n"
                "( column1 int )"
                "default character set=utf8"
            )]
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.name, 'foo')
            self.assertEqual(tbl.charset, 'utf8')

    def test_bare_column_collate(self):
        sql = (
            "Create table foo\n"
            "( column1 varchar(10) collate latin1_bin )"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(tbl.column_list[0].collate, 'latin1_bin')

    def test_bare_column_charset(self):
        sql = (
            "Create table foo\n"
            "( column1 varchar(10) character set latin1 )"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(tbl.column_list[0].charset, 'latin1')

    def test_table_collate(self):
        sqls = [
            (
                "Create table foo\n"
                "( column1 int )"
                "collate='utf8_bin'"
            ), (
                "Create table foo\n"
                "( column1 int )"
                "default collate='utf8_bin'"
            )]
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.name, 'foo')
            self.assertEqual(tbl.collate, 'utf8_bin')

    def test_table_key_block_size(self):
        sql = (
            "Create table foo\n"
            "( column1 int )"
            "key_block_size=16"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(tbl.key_block_size, 16)

    def test_engine(self):
        sql = (
            "Create table foo\n"
            "( column1 int )"
            "engine=Innodb"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(tbl.engine, 'INNODB')

    def test_table_incre(self):
        sql = (
            "Create table foo\n"
            "( column1 int )"
            "auto_increment=123"
        )
        tbl = parse_create(sql)
        self.assertEqual(tbl.name, 'foo')
        self.assertEqual(tbl.auto_increment, 123)

    def test_row_format(self):
        sqls = [
            (
                "Create table foo\n"
                "( column1 int )"
                "row_format=compressed"
            )]
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.name, 'foo')
            self.assertEqual(tbl.row_format, 'COMPRESSED')

    def test_compression(self):
        sqls = [
            (
                "Create table foo\n"
                "( column1 int )"
                "compression=zlib_stream"
            )]
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.name, 'foo')
            self.assertEqual(tbl.compression, 'ZLIB_STREAM')

    def test_simple_with_all_supported_int_type_and_length(self):
        supported_type = [
            'int', 'tinyint', 'bigint', 'mediumint', 'smallint'
        ]
        for col_type in supported_type:
            for unsigned in ['unsigned', '']:
                sql = (
                    "Create table foo\n"
                    "( column1 {}(10) {})".format(col_type, unsigned)
                )
                tbl = parse_create(sql)
                self.assertTrue(tbl.primary_key.is_unique)
                self.assertEqual(len(tbl.column_list), 1)
                self.assertEqual(tbl.column_list[0].name, 'column1')
                self.assertEqual(tbl.column_list[0].length, str(10))
                self.assertEqual(
                    tbl.column_list[0].column_type, col_type.upper())
                self.assertEqual(tbl.name, 'foo')

    def test_simple_create_table_with_inline_pri(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int primary )"
        )
        sqls.append(
            "Create table foo\n"
            "( column1 int primary key)"
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertTrue(tbl.primary_key.is_unique)
            self.assertEqual(len(tbl.primary_key.column_list), 1)
            self.assertEqual(tbl.primary_key.column_list[0].name, 'column1')
            self.assertEqual(tbl.name, 'foo')

    def test_trailing_pri(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int , "
            "primary key (column1))"
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertTrue(tbl.primary_key.is_unique)
            self.assertEqual(len(tbl.primary_key.column_list), 1)
            self.assertEqual(tbl.primary_key.column_list[0].name, 'column1')
            self.assertEqual(tbl.name, 'foo')

    def test_docstore_index(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( a document , "
            "KEY key_name ( `a`.`b`.`c` AS INT ))"
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertTrue(len(tbl.indexes), 1)
            self.assertTrue(len(tbl.indexes[0].column_list), 1)
            self.assertEqual(tbl.indexes[0].column_list[0].document_path,
                             '`a`.`b`.`c`')
            self.assertEqual(tbl.indexes[0].column_list[0].key_type, 'INT')

    def test_docstore_index_str(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( a document , "
            "KEY key_name ( `a`.`b`.`c` AS STRING(10) ))"
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertTrue(len(tbl.indexes), 1)
            self.assertTrue(len(tbl.indexes[0].column_list), 1)
            self.assertEqual(tbl.indexes[0].column_list[0].document_path,
                             '`a`.`b`.`c`')
            self.assertEqual(tbl.indexes[0].column_list[0].key_type, 'STRING')
            self.assertEqual(tbl.indexes[0].column_list[0].length, str(10))

    def test_multiple_tailing_index(self):
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
            tbl = parse_create(sql)
            self.assertEqual(len(tbl.indexes), 2)
            self.assertEqual(len(tbl.indexes[0].column_list), 2)
            self.assertEqual(len(tbl.indexes[1].column_list), 1)
            self.assertFalse(tbl.indexes[0].is_unique)
            self.assertEqual(tbl.indexes[0].column_list[0].name, 'column1')
            self.assertEqual(tbl.indexes[0].column_list[1].name, 'column2')
            self.assertEqual(tbl.indexes[0].comment, "'a comment'")
            self.assertEqual(tbl.indexes[1].column_list[0].name, 'column1')
            self.assertTrue(tbl.indexes[1].is_unique)

    def test_fulltext_index(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int, \n"
            "column2 varchar(10),\n"
            " FULLTEXT key `index_name` (column1 )\n"
            ")"
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(len(tbl.indexes), 1)
            self.assertEqual(len(tbl.indexes[0].column_list), 1)
            self.assertFalse(tbl.indexes[0].is_unique)
            self.assertEqual(tbl.indexes[0].name, 'index_name')
            self.assertEqual(tbl.indexes[0].key_type, 'FULLTEXT')

    def test_default_value_int(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int default 0) "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].default, "0")

    def test_default_value_string(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int default '0') "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].default, "'0'")

    def test_default_value_empty_string(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 char(1) default '') "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].default, "''")

    def test_nullable(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int null) "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertTrue(tbl.column_list[0].nullable)

    def test_explicit_nullable(self):
        # explicitly specify nullable and implicitly should be identical
        left = (
            "Create table foo\n"
            "( column1 int null) "
        )
        right = (
            "Create table foo\n"
            "( column1 int) "
        )
        left_obj = parse_create(left)
        right_obj = parse_create(right)
        self.assertEqual(left_obj, right_obj)

    def test_not_nullable(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int not null) "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertFalse(tbl.column_list[0].nullable)

    def test_col_type_timestamp(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 timestamp default current_timestamp) "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].default, 'CURRENT_TIMESTAMP')

    def test_col_type_timestamp_on_update(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 timestamp(10) default current_timestamp "
            "on update current_timestamp) "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].default, 'CURRENT_TIMESTAMP')
            self.assertEqual(tbl.column_list[0].on_update_current_timestamp,
                             'CURRENT_TIMESTAMP')

    def test_explicit_timestamp_default_for_bare_timestamp(self):
        left = (
            "Create table foo\n"
            "( column1 timestamp(10) ) "
        )
        right = (
            "Create table foo\n"
            "( column1 timestamp(10) NOT NULL default current_timestamp "
            "on update current_timestamp) "
        )
        self.assertEqual(parse_create(left), parse_create(right))

    def test_explicit_timestamp_default_for_not_null(self):
        left = (
            "Create table foo\n"
            "( column1 timestamp(10) NOT NULL ) "
        )
        right = (
            "Create table foo\n"
            "( column1 timestamp(10) NOT NULL default current_timestamp "
            "on update current_timestamp) "
        )
        self.assertEqual(parse_create(left), parse_create(right))

    def test_no_accidentally_explicit_timestamp_default_for(self):
        left = (
            "Create table foo\n"
            "( column1 timestamp(10) NULL ) "
        )
        right = (
            "Create table foo\n"
            "( column1 timestamp(10) NOT NULL default current_timestamp "
            "on update current_timestamp) "
        )
        self.assertNotEqual(parse_create(left), parse_create(right))

    def test_col_collate(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 varchar(10) collate utf8_bin) "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].collate, 'utf8_bin')

    def test_col_charset(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 varchar(10) character set utf8) "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].charset, 'utf8')

    def test_col_comment(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int comment 'column comment') "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].comment, "'column comment'")

    def test_default_value_bit(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 bit default b'0') "
        )
        for sql in sqls:
            tbl = parse_create(sql)
            self.assertEqual(tbl.column_list[0].default, "'0'")
            self.assertTrue(tbl.column_list[0].is_default_bit, "'0'")

    def test_foreign_key(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int primary key, "
            "foreign key (column1) references table2 (column1))"
        )
        sqls.append(
            "Create table foo\n"
            "( column1 int primary key, "
            "constraint `key_with_name` foreign key (column1) "
            "references table2 (column1))"
        )
        sqls.append(
            "Create table foo\n"
            "( column1 int primary key, "
            "constraint foreign key (column1) "
            "references table2 (column1))"
        )
        for sql in sqls:
            sql_obj = parse_create(sql)
            self.assertTrue(sql_obj.constraint is not None)

    def test_multiple_primary(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 int primary key, "
            "primary key (column1))"
        )
        for sql in sqls:
            with self.assertRaises(ParseError):
                parse_create(sql)

    def test_to_sql_consistency(self):
        sqls = []
        sqls.append(
            "Create table foo\n"
            "( column1 bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (column1) ,"
            "key `aname` (column1, column2(19)) )"
        )
        for sql in sqls:
            tbl = parse_create(sql).to_sql()
            str_after_parse = parse_create(tbl).to_sql()
            self.assertEqual(tbl, str_after_parse)

    def test_to_sql_consistency_with_backtick(self):
        sqls = []
        sqls.append(
            "Create table `foo``bar`\n"
            "( `column``1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (`column``1`) ,"
            "key `a``name` (column1, column2(19)) )"
        )
        for sql in sqls:
            tbl = parse_create(sql).to_sql()
            str_after_parse = parse_create(tbl).to_sql()
            self.assertEqual(tbl, str_after_parse)

    def test_inequallity_in_index_col_length(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertNotEqual(left.indexes, right.indexes)

    def test_inequallity_in_col_type(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertNotEqual(left.column_list, right.column_list)

    def test_inequallity_in_col_default(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertNotEqual(left.column_list, right.column_list)

    def test_inequallity_in_col_name(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertNotEqual(left.column_list, right.column_list)

    def test_inequallity_in_index_length(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertNotEqual(left.indexes, right.indexes)

    def test_inequallity_in_set(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertNotEqual(left, right)

    def test_inequallity_in_enum(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertNotEqual(left, right)

    def test_identical_equallity(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertEqual(left.indexes, right.indexes)
        self.assertEqual(left.column_list, right.column_list)
        self.assertEqual(left, right)
        self.assertFalse(left != right)

    def test_implicit_default_for_nullable(self):
        left = (
            "Create table `foobar`\n"
            "( `column1` int ,"
            "PRIMARY key (`column1`))"
        )
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertEqual(left, right)

        right = parse_create(right_default)
        self.assertNotEqual(left, right)

    def test_implicit_quote_for_default(self):
        # numeric defaults will automatically be quoted by MySQL, so they
        # are the same
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertEqual(left, right)

    def test_implicit_quote_for_not_nulldefault(self):
        # numeric defaults will automatically be quoted by MySQL, so they
        # are the same
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertEqual(left, right)


class ModelTableTestCase(unittest.TestCase):
    def test_identical_table_checksum(self):
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertEqual(left.checksum, right.checksum)

    def test_different_cases_in_type(self):
        """
        Upper/Lower case in column type shouldn't affect the value of checksum
        """
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
        left = parse_create(left)
        right = parse_create(right)
        self.assertEqual(left.checksum, right.checksum)

    def test_tables_with_different_idx_seq_equal(self):
        """
        index sequence shouldn't affect the fact that two tables are identical
        """
        left = (
            "Create table `foobar` "
            "( `column1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "key (`column1`) ,"
            "key2 (`column2`) )"
        )
        right = (
            "Create table `foobar` "
            "( `column1` bit default b'0',"
            " column2 varchar(100) default 'abc',"
            " column3 timestamp on update current_timestamp,"
            " column4 enum('a','b'),"
            " column5 set('a','b'),"
            "key2 (`column2`) ,"
            "key (`column1`) )"
        )
        left_obj = parse_create(left)
        right_obj = parse_create(right)
        self.assertEqual(left_obj, right_obj)

    def test_droppable_index(self):
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
            "unique key `idx_name2` (column1))"
            "key (auto_inc))"
        )
        sql_obj = parse_create(sql)
        # both `idx_name` `idx_name2` are droppable
        droppable_indexes = sql_obj.droppable_indexes(keep_unique_key=False)
        self.assertEqual(len(droppable_indexes), 2)
        for idx in droppable_indexes:
            self.assertTrue(idx.name in ['idx_name', 'idx_name2'])

        # only `idx_name` is droppable
        droppable_indexes = sql_obj.droppable_indexes(keep_unique_key=True)
        self.assertEqual(len(droppable_indexes), 1)
        for idx in droppable_indexes:
            self.assertEqual(idx.name, 'idx_name')

    def test_is_myrocks_ttl_table(self):
        sql = (
            "Create table `foo`\n"
            "( "
            " `id` int auto_increment,"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (id) COMMENT 'this is pk' "
            ") ENGINE=ROCKSDB COMMENT='ttl_duration=123;' "
        )
        sql_obj = parse_create(sql)
        self.assertTrue(sql_obj.is_myrocks_ttl_table)

    def test_is_myrocks_ttl_table_partition_ttl(self):
        sql = (
            "Create table `foo`\n"
            "( "
            " `id` int auto_increment,"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (id) COMMENT 'P1_cfname=p1_v1' "
            ") ENGINE=ROCKSDB COMMENT='P1_ttl_duration=123;' "
        )
        sql_obj = parse_create(sql)
        self.assertTrue(sql_obj.is_myrocks_ttl_table)

    def test_is_myrocks_ttl_table_not_ttl(self):
        sql = (
            "Create table `foo`\n"
            "( "
            " `id` int auto_increment,"
            " column2 varchar(100) default 'abc',"
            "PRIMARY key (id) COMMENT 'P1_cfname=p1_v1' "
            ") ENGINE=ROCKSDB COMMENT='some other comment' "
        )
        sql_obj = parse_create(sql)
        self.assertFalse(sql_obj.is_myrocks_ttl_table)
