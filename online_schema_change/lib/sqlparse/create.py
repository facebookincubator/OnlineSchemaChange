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

from . import models
from pyparsing import (
    Word, Literal, Optional, nums, Group, CaselessLiteral,
    alphanums, ZeroOrMore, QuotedString, Combine, ParseException,
    SkipTo, StringEnd, upcaseTokens
)
import logging

log = logging.getLogger(__name__)


__all__ = ['parse_create', 'ParseError']


class ParseError(Exception):
    def __init__(self, msg, line=0, column=0):
        self._msg = msg
        self._line = line
        self._column = column

    def __str__(self):
        return "Line: {}, Column: {}\n {}".format(
            self._line, self._column, self._msg)


class CreateParser(object):
    """
    This class can take a plain "CREATE TABLE" SQL as input and parse it into
    a Table object, so that we have more insight on the detail of this SQL.

    Example:
    sql = 'create table foo ( bar int primary key )'
    parser = CreateParser(sql)
    try:
        tbl_obj = parser.parse()
    except ParseError:
        log.error("Failed to parse SQL")

    This set of BNF rules are basically translated from the MySQL manual:
    http://dev.mysql.com/doc/refman/5.6/en/create-table.html
    If you don't know how to change the rule or fix the bug,
    <Getting Started with Pyparsing> is probably the best book to start with.
    Also this wiki has all supported functions listed:
    https://pyparsing.wikispaces.com/HowToUsePyparsing
    If you want have more information how these characters are
    matching, add .setDebug(True) after the specific token you want to debug
    """
    # Basic token
    WORD_CREATE = CaselessLiteral("CREATE").suppress()
    WORD_TABLE = CaselessLiteral("TABLE").suppress()
    COMMA = Literal(',').suppress()
    DOT = Literal('.')
    LEFT_PARENTHESES = Literal('(').suppress()
    RIGHT_PARENTHESES = Literal(')').suppress()
    QUOTE = Literal("'") | Literal('"')
    BACK_QUOTE = Optional(Literal('`')).suppress()
    LENGTH = Word(nums)
    OBJECT_NAME = Word(alphanums + "_" + "-" + "<" + ">" + ":")
    QUOTED_STRING_WITH_QUOTE = QuotedString(
        quoteChar="'", escQuote="''", escChar='\\', multiline=True,
        unquoteResults=False
    ) | QuotedString(
        quoteChar='"', escQuote='""', escChar='\\', multiline=True,
        unquoteResults=False
    )
    QUOTED_STRING = QuotedString(
        quoteChar="'", escQuote="''", escChar='\\', multiline=True
    ) | QuotedString(
        quoteChar='"', escQuote='""', escChar='\\', multiline=True
    )
    # Start of a create table statement
    # Sample: this part of rule will match following section
    # `table_name` IF NOT EXISTS
    IF_NOT_EXIST = Optional(
        CaselessLiteral("IF") + CaselessLiteral("NOT") +
        CaselessLiteral("EXISTS")
    ).suppress()
    TABLE_NAME = (
        QuotedString(
            quoteChar="`", escQuote="``", escChar='\\', unquoteResults=True
        ) | OBJECT_NAME
    )('table_name')

    # Column definition
    # Sample: this part of rule will match following section
    # `id` bigint(20) unsigned NOT NULL DEFAULT '0',
    COLUMN_NAME = (
        QuotedString(
            quoteChar="`", escQuote="``", escChar='\\', unquoteResults=True
        ) | OBJECT_NAME
    )('column_name')
    COLUMN_NAME_WITH_QUOTE = (
        QuotedString(
            quoteChar="`", escQuote="``", escChar='\\', unquoteResults=False
        ) | OBJECT_NAME
    )('column_name')
    UNSIGNED = Optional(CaselessLiteral("UNSIGNED"))('unsigned')
    ZEROFILL = Optional(CaselessLiteral("ZEROFILL"))('zerofill')
    COL_LEN = Combine(
        LEFT_PARENTHESES + LENGTH + RIGHT_PARENTHESES,
        adjacent=False
    )('length')
    INT_TYPE = (
        CaselessLiteral("TINYINT") | CaselessLiteral("SMALLINT") |
        CaselessLiteral("MEDIUMINT") | CaselessLiteral("INT") |
        CaselessLiteral("INTERGER") | CaselessLiteral("BIGINT") |
        CaselessLiteral("BINARY") | CaselessLiteral("BIT")
    )
    INT_DEF = (
        INT_TYPE('column_type') + Optional(COL_LEN) + UNSIGNED +
        ZEROFILL
    )
    VARBINARY_DEF = (
        CaselessLiteral('VARBINARY')('column_type') + COL_LEN
    )
    FLOAT_TYPE = \
        CaselessLiteral("REAL") | CaselessLiteral("DOUBLE") |\
        CaselessLiteral("FLOAT") | CaselessLiteral("DECIMAL") |\
        CaselessLiteral("NUMERIC")
    FLOAT_LEN = Combine(
        LEFT_PARENTHESES + LENGTH +
        Optional(COMMA + LENGTH) + RIGHT_PARENTHESES,
        adjacent=False,
        joinString=', '
    )('length')
    FLOAT_DEF = (
        FLOAT_TYPE('column_type') + Optional(FLOAT_LEN) + UNSIGNED +
        ZEROFILL
    )
    # time type definition. They contain type_name and an optional FSP section
    # Sample: DATETIME[(fsp)]
    FSP = COL_LEN
    DT_DEF = (
        Combine(
            CaselessLiteral("TIME") + Optional(CaselessLiteral("STAMP"))
        ) |
        CaselessLiteral("DATETIME")
    )('column_type') + Optional(FSP)
    SIMPLE_DEF = (
        CaselessLiteral("DATE") | CaselessLiteral("YEAR") |
        CaselessLiteral("TINYBLOB") | CaselessLiteral("BLOB") |
        CaselessLiteral("MEDIUMBLOB") | CaselessLiteral("LONGBLOB") |
        CaselessLiteral("BOOL") | CaselessLiteral("BOOLEAN")
    )('column_type')
    OPTIONAL_COL_LEN = Optional(COL_LEN)
    BINARY = Optional(CaselessLiteral("BINARY"))('binary')
    CHARSET_NAME = (
        Optional(QUOTE).suppress() +
        Word(alphanums + '_')('charset') +
        Optional(QUOTE).suppress()
    )
    COLLATION_NAME = (
        Optional(QUOTE).suppress() +
        Word(alphanums + '_')('collate') +
        Optional(QUOTE).suppress()
    )
    CHARSET_DEF = Optional(
        CaselessLiteral("CHARACTER SET").suppress() + CHARSET_NAME
    )
    COLLATE_DEF = Optional(
        CaselessLiteral("COLLATE").suppress() + COLLATION_NAME
    )
    CHAR_DEF = (
        CaselessLiteral("CHAR")('column_type') + OPTIONAL_COL_LEN + BINARY +
        CHARSET_DEF + COLLATE_DEF
    )
    VARCHAR_DEF = (
        CaselessLiteral("VARCHAR")('column_type') + COL_LEN + BINARY +
        CHARSET_DEF + COLLATE_DEF
    )
    TEXT_TYPE = (
        CaselessLiteral("TINYTEXT") | CaselessLiteral("TEXT") |
        CaselessLiteral("MEDIUMTEXT") | CaselessLiteral("LONGTEXT") |
        CaselessLiteral("DOCUMENT")
    )
    TEXT_DEF = (
        TEXT_TYPE('column_type') + BINARY + CHARSET_DEF + COLLATE_DEF
    )
    ENUM_VALUE_LIST = Group(
        QUOTED_STRING_WITH_QUOTE + ZeroOrMore(COMMA + QUOTED_STRING_WITH_QUOTE)
    )('enum_value_list')
    ENUM_DEF = (
        CaselessLiteral("ENUM")('column_type') +
        LEFT_PARENTHESES + ENUM_VALUE_LIST + RIGHT_PARENTHESES +
        CHARSET_DEF + COLLATE_DEF
    )
    SET_VALUE_LIST = Group(
        QUOTED_STRING_WITH_QUOTE + ZeroOrMore(COMMA + QUOTED_STRING_WITH_QUOTE)
    )('set_value_list')
    SET_DEF = (
        CaselessLiteral("SET")('column_type') +
        LEFT_PARENTHESES + SET_VALUE_LIST + RIGHT_PARENTHESES +
        CHARSET_DEF + COLLATE_DEF
    )
    DATA_TYPE = (
        INT_DEF | FLOAT_DEF | DT_DEF |
        SIMPLE_DEF |
        TEXT_DEF | CHAR_DEF | VARCHAR_DEF |
        ENUM_DEF | SET_DEF | VARBINARY_DEF
    )

    # Column attributes come after column type and length
    NULLABLE = (
        CaselessLiteral("NULL") | CaselessLiteral("NOT NULL")
    )
    DEFAULT_VALUE = (
        CaselessLiteral("DEFAULT").suppress() +
        (
            Optional(Literal('b'))('is_bit') +
            QUOTED_STRING_WITH_QUOTE('default') |
            Combine(
                CaselessLiteral("CURRENT_TIMESTAMP")('default') +
                Optional(COL_LEN)('ts_len')
            ) |
            Word(alphanums + '_' + '-' + '+')('default')
        )
    )
    ON_UPDATE = (
        CaselessLiteral("ON") +
        CaselessLiteral("UPDATE") +
        (
            CaselessLiteral("CURRENT_TIMESTAMP")('on_update') +
            Optional(COL_LEN)('on_update_ts_len')
        )
    )
    AUTO_INCRE = CaselessLiteral("AUTO_INCREMENT")
    UNIQ_KEY = (
        CaselessLiteral("UNIQUE") +
        Optional(CaselessLiteral("KEY")).suppress()
    )
    PRIMARY_KEY = (
        CaselessLiteral("PRIMARY") +
        Optional(CaselessLiteral("KEY")).suppress()
    )
    COMMENT = Combine(
        CaselessLiteral("COMMENT").suppress() +
        QUOTED_STRING_WITH_QUOTE,
        adjacent=False
    )
    COLUMN_DEF = Group(
        COLUMN_NAME + DATA_TYPE + ZeroOrMore(
            NULLABLE('nullable') |
            DEFAULT_VALUE |
            ON_UPDATE |
            AUTO_INCRE('auto_increment') |
            UNIQ_KEY('uniq_key') |
            PRIMARY_KEY('primary') |
            COMMENT('comment')
        )
    )
    COLUMN_LIST = Group(
        COLUMN_DEF + ZeroOrMore(COMMA + COLUMN_DEF)
    )('column_list')

    DOCUMENT_PATH = Combine(
        COLUMN_NAME_WITH_QUOTE + ZeroOrMore(DOT + COLUMN_NAME_WITH_QUOTE))
    IDX_COL = ((
        Group(
            DOCUMENT_PATH + CaselessLiteral('AS') +
            (CaselessLiteral('INT') | CaselessLiteral('STRING')) +
            Optional(COL_LEN, default='')
        )
    ) | (
        Group(COLUMN_NAME + Optional(COL_LEN, default=''))
    ))

    # Primary key section
    COL_NAME_LIST = Group(
        IDX_COL + ZeroOrMore(COMMA + IDX_COL)
    )
    IDX_COLS = (
        LEFT_PARENTHESES + COL_NAME_LIST + RIGHT_PARENTHESES
    )
    WORD_PRI_KEY = (
        CaselessLiteral("PRIMARY").suppress() +
        CaselessLiteral("KEY").suppress()
    )
    KEY_BLOCK_SIZE = (
        CaselessLiteral("KEY_BLOCK_SIZE").suppress() +
        Optional(Literal('=')) + Word(nums)('idx_key_block_size')
    )
    INDEX_USING = (
        CaselessLiteral("USING").suppress() +
        (CaselessLiteral("BTREE") | CaselessLiteral("HASH"))('idx_using')
    )

    INDEX_OPTION = (
        ZeroOrMore(KEY_BLOCK_SIZE | COMMENT('idx_comment') | INDEX_USING)
    )
    PRI_KEY_DEF = (
        COMMA + WORD_PRI_KEY + IDX_COLS('pri_list') +
        INDEX_OPTION
    )

    # Index section
    KEY_TYPE = (
        CaselessLiteral("FULLTEXT") | CaselessLiteral("SPATIAL")
    )('key_type')
    WORD_UNIQUE = CaselessLiteral("UNIQUE")('unique')
    WORD_KEY = (
        CaselessLiteral("INDEX").suppress() |
        CaselessLiteral("KEY").suppress()
    )
    IDX_NAME = Optional(COLUMN_NAME)
    IDX_DEF = (ZeroOrMore(
        Group(
            COMMA + Optional(WORD_UNIQUE | KEY_TYPE) +
            WORD_KEY + IDX_NAME('index_name') +
            IDX_COLS('index_col_list') + INDEX_OPTION
        )
    ))('index_section')

    # Constraint section as this is not a recommended way of using MySQL
    # we'll treat the whole section as a string
    CONSTRAINT = Combine(
        ZeroOrMore(
            COMMA +
            Optional(CaselessLiteral('CONSTRAINT')) +
            # foreign key name except the key word 'FOREIGN'
            Optional((~CaselessLiteral('FOREIGN') + COLUMN_NAME)) +
            CaselessLiteral('FOREIGN') + CaselessLiteral('KEY') +
            LEFT_PARENTHESES + COL_NAME_LIST + RIGHT_PARENTHESES +
            CaselessLiteral('REFERENCES') + COLUMN_NAME +
            LEFT_PARENTHESES + COL_NAME_LIST + RIGHT_PARENTHESES +
            ZeroOrMore(Word(alphanums))
        ),
        adjacent=False,
        joinString=' '
    )('constraint')

    # Table option section
    ENGINE = (
        CaselessLiteral("ENGINE").suppress() +
        Optional(Literal('=')).suppress() +
        COLUMN_NAME('engine').setParseAction(upcaseTokens)
    )
    DEFAULT_CHARSET = (
        Optional(CaselessLiteral("DEFAULT")).suppress() +
        ((
            CaselessLiteral("CHARACTER").suppress() +
            CaselessLiteral("SET").suppress()
        ) | (
            CaselessLiteral("CHARSET").suppress()
        )) +
        Optional(Literal('=')).suppress() +
        Word(alphanums + '_')('charset')
    )
    TABLE_COLLATE = (
        Optional(CaselessLiteral("DEFAULT")).suppress() +
        CaselessLiteral("COLLATE").suppress() +
        Optional(Literal('=')).suppress() + COLLATION_NAME
    )
    ROW_FORMAT = (
        CaselessLiteral("ROW_FORMAT").suppress() +
        Optional(Literal('=')).suppress() +
        Word(alphanums + '_')('row_format').setParseAction(upcaseTokens)
    )
    TABLE_KEY_BLOCK_SIZE = (
        CaselessLiteral("KEY_BLOCK_SIZE").suppress() +
        Optional(Literal('=')).suppress() +
        Word(nums)('key_block_size').setParseAction(
            lambda s, l, t: [int(t[0])])
    )
    COMPRESSION = (
        CaselessLiteral("COMPRESSION").suppress() +
        Optional(Literal('=')).suppress() +
        Word(alphanums + '_')('compression').setParseAction(upcaseTokens)
    )
    # Parse and make sure auto_increment is an interger
    # parseAction function is defined as fn( s, loc, toks ), where:
    # s is the original parse string
    # loc is the location in the string where matching started
    # toks is the list of the matched tokens, packaged as a ParseResults_
    # object
    TABLE_AUTO_INCRE = (
        CaselessLiteral("AUTO_INCREMENT").suppress() +
        Optional(Literal('=')).suppress() +
        Word(nums)('auto_increment')
        .setParseAction(
            lambda s, l, t: [int(t[0])])
    )
    TABLE_COMMENT = (
        CaselessLiteral("COMMENT").suppress() +
        Optional(Literal('=')).suppress() + QUOTED_STRING_WITH_QUOTE('comment')
    )
    TABLE_OPTION = ZeroOrMore(
        ENGINE | DEFAULT_CHARSET |
        TABLE_COLLATE |
        ROW_FORMAT | TABLE_KEY_BLOCK_SIZE |
        COMPRESSION | TABLE_AUTO_INCRE |
        TABLE_COMMENT
    )

    # Partition section
    PARTITION = Optional(Combine(
        Combine(Optional(Literal('/*!') + Word(nums))) +
        CaselessLiteral("PARTITION") + CaselessLiteral("BY") +
        SkipTo(StringEnd()),
        adjacent=False,
        joinString=" "
    )('partition'))

    @classmethod
    def generate_rule(cls):
        # The final rule for the whole statement match
        return (
            cls.WORD_CREATE + cls.WORD_TABLE + cls.IF_NOT_EXIST +
            cls.TABLE_NAME +
            cls.LEFT_PARENTHESES + cls.COLUMN_LIST +
            Optional(cls.PRI_KEY_DEF) + cls.IDX_DEF +
            cls.CONSTRAINT +
            cls.RIGHT_PARENTHESES + cls.TABLE_OPTION('table_options') +
            cls.PARTITION
        )

    @classmethod
    def parse(cls, sql):
        try:
            result = cls.generate_rule().parseString(sql)
        except ParseException as e:
            raise ParseError(
                "Failed to parse SQL, unsupported syntax: {}"
                .format(e), e.line, e.column)

        inline_pri_exists = False
        table = models.Table()
        table.name = result.table_name
        table_options = [
            'engine', 'charset', 'collate', 'row_format',
            'key_block_size', 'compression', 'auto_increment',
            'comment'
        ]
        for table_option in table_options:
            if table_option in result.table_options:
                setattr(table, table_option,
                        result.table_options.get(table_option))
        if 'partition' in result:
            table.partition = result.partition
        if 'constraint' in result:
            table.constraint = result.constraint
        for column_def in result.column_list:
            if column_def.column_type == 'ENUM':
                column = models.EnumColumn()
                for enum_value in column_def.enum_value_list:
                    column.enum_list.append(enum_value)
            elif column_def.column_type == 'SET':
                column = models.SetColumn()
                for set_value in column_def.set_value_list:
                    column.set_list.append(set_value)
            elif column_def.column_type in ('TIMESTAMP', 'DATETIME'):
                column = models.TimestampColumn()
                if 'on_update' in column_def:
                    if 'on_update_ts_len' in column_def:
                        column.on_update_current_timestamp = \
                            "{}({})".format(
                                column_def.on_update,
                                column_def.on_update_ts_len)
                    else:
                        column.on_update_current_timestamp = \
                            column_def.on_update
            else:
                column = models.Column()

            column.name = column_def.column_name
            column.column_type = column_def.column_type

            # We need to check whether each column property exist in the
            # create table string, because not specifying a "COMMENT" is
            # different from specifying "COMMENT" equals to empty string.
            # The former one will ends up being
            #   column=None
            # and the later one being
            #   column=''
            if 'comment' in column_def:
                column.comment = column_def.comment
            if 'nullable' in column_def:
                if column_def.nullable == 'NULL':
                    column.nullable = True
                elif column_def.nullable == 'NOT NULL':
                    column.nullable = False
            if 'unsigned' in column_def:
                if column_def.unsigned == 'UNSIGNED':
                    column.unsigned = True
            if 'default' in column_def:
                if 'ts_len' in column_def:
                    column.default = "{}({})".format(
                        column_def.default, column_def.ts_len)
                else:
                    column.default = column_def.default
                if 'is_bit' in column_def:
                    column.is_default_bit = True
            if 'charset' in column_def:
                column.charset = column_def.charset
            if 'length' in column_def:
                column.length = column_def.length
            if 'collate' in column_def:
                column.collate = column_def.collate
            if 'auto_increment' in column_def:
                column.auto_increment = True
            if 'primary' in column_def:
                idx_col = models.IndexColumn()
                idx_col.name = column_def.column_name
                table.primary_key.column_list.append(idx_col)
                inline_pri_exists = True
            table.column_list.append(column)
        if 'pri_list' in result:
            if inline_pri_exists:
                raise ParseError("Multiple primary keys defined")
            table.primary_key.name = 'PRIMARY'
            for col in result.pri_list:
                for name, length in col:
                    idx_col = models.IndexColumn()
                    idx_col.name = name
                    if length:
                        idx_col.length = length
                    table.primary_key.column_list.append(idx_col)
            if 'idx_key_block_size' in result:
                table.primary_key.key_block_size = result.pri_key_block_size
            if 'idx_comment' in result:
                table.primary_key.comment = result.idx_comment
        if 'index_section' in result:
            for idx_def in result.index_section:
                idx = models.TableIndex()
                idx.name = idx_def.index_name
                if 'idx_key_block_size' in idx_def:
                    idx.key_block_size = idx_def.idx_key_block_size
                if 'idx_comment' in idx_def:
                    idx.comment = idx_def.idx_comment
                if 'idx_using' in idx_def:
                    idx.using = idx_def.idx_using
                if 'key_type' in idx_def:
                    idx.key_type = idx_def.key_type
                if 'unique' in idx_def:
                    idx.is_unique = True
                for col in idx_def.index_col_list:
                    for col_def in col:
                        if len(col_def) == 4 and col_def[1].upper() == 'AS':
                            (document_path, word_as, key_type, length) = col_def
                            idx_col = models.DocStoreIndexColumn()
                            idx_col.document_path = document_path
                            idx_col.key_type = key_type
                            if length:
                                idx_col.length = length
                            idx.column_list.append(idx_col)
                        else:
                            (name, length) = col_def
                            idx_col = models.IndexColumn()
                            idx_col.name = name
                            if length:
                                idx_col.length = length
                            idx.column_list.append(idx_col)
                table.indexes.append(idx)
        return table


def parse_create(sql):
    return CreateParser.parse(sql)
