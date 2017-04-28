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

import hashlib


def escape(keyword):
    """
    Escape the backtick for keyword when generating an actual SQL.
    """
    return keyword.replace('`', '``')


def is_equal(left, right):
    """
    If both left and right are None, then they are equal because both haven't
    been initialized yet.
    If only one of them is None, they they are not equal
    If both of them is not None, then it's possible they are equal, and we'll
    return True and do some more comparision later
    """
    if left is not None and right is not None:
        # Neither of them is None
        if left != right:
            return False
        else:
            return True
    elif left is None and right is not None:
        # Only left is None
        return False
    elif left is not None and right is None:
        # Only right is None
        return False
    else:
        # Both of them are None
        return True


class IndexColumn(object):
    """
    A column definition inside index section.
    This is different from a table column definition, because only `name` and
    `length` is required for a index column definition
    """
    def __init__(self):
        self.name = None
        self.length = None

    def __str__(self):
        if self.length is not None:
            return "{}({})".format(self.name, self.length)
        else:
            return "{}".format(self.name)

    def __eq__(self, other):
        if self.name != other.name:
            return False
        if self.length is not None:
            if other.length is None:
                return False
            elif self.length != other.length:
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        if self.length is not None:
            return "`{}`({})".format(escape(self.name), self.length)
        else:
            return "`{}`".format(escape(self.name))


class DocStoreIndexColumn(object):
    """
    A column definition inside index section for DocStore.
    DocStore index column has more attributes than the normal one
    """
    def __init__(self):
        self.document_path = None
        self.key_type = None
        self.length = None

    def __str__(self):
        if self.length is not None:
            return "{} AS {}({})".format(
                self.document_path, self.key_type, self.length)
        else:
            return "{} AS {}".format(self.document_path, self.key_type)

    def __eq__(self, other):
        for attr in ('document_path', 'key_type', 'length'):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        if self.length is not None:
            return "{} AS {}({})".format(
                self.document_path, self.key_type, self.length)
        else:
            return "{} AS {}".format(self.document_path, self.key_type)


class TableIndex(object):
    """
    An index definition. This can defined either directly after single column
    definition or after all column definitions
    """
    def __init__(self, name=None, is_unique=False):
        self.name = name
        self.key_block_size = None
        self.comment = None
        self.is_unique = is_unique
        self.key_type = None
        self.using = None
        self.column_list = []

    def __str__(self):
        idx_str = []
        idx_str.append("NAME: {}".format(self.name))
        idx_str.append("IS UNIQUE: {}".format(self.is_unique))
        idx_str.append("TYPE: {}".format(self.key_type))
        col_list_str = []
        for col_str in self.column_list:
            col_list_str.append(str(col_str))
        if self.using:
            idx_str.append("USING: {}".format(self.using))
        idx_str.append("KEY LIST: {}".format(','.join(col_list_str)))
        idx_str.append("KEY_BLOCK_SIZE: {}".format(self.key_block_size))
        idx_str.append("COMMENT: {}".format(self.comment))
        return '/ '.join(idx_str)

    def __eq__(self, other):
        for attr in (
                'name', 'key_block_size', 'comment', 'is_unique', 'key_type',
                'using'):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        return self.column_list == other.column_list

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        segments = []
        if self.name is not None:
            if self.name == 'PRIMARY':
                segments.append('PRIMARY KEY')
            else:
                if self.is_unique:
                    segments.append('UNIQUE KEY `{}`'
                                    .format(escape(self.name)))
                elif self.key_type is not None:
                    segments.append(
                        '{} KEY `{}`'.format(self.key_type,
                                             escape(self.name)))
                else:
                    segments.append('KEY `{}`'.format(escape(self.name)))
        else:
            segments.append('KEY')
        if self.using is not None:
            segments.append('USING {}'.format(self.using))

        segments.append(
            "({})"
            .format(', '.join([col.to_sql() for col in self.column_list]))
        )

        if self.key_block_size is not None:
            segments.append('KEY_BLOCK_SIZE={}'.format(self.key_block_size))
        if self.comment is not None:
            segments.append('COMMENT {}'.format(self.comment))
        return ' '.join(segments)


class Column(object):
    """
    Representing a column definiton in a table
    """
    def __init__(self):
        self.name = None
        self.column_type = None
        self.default = None
        self.charset = None
        self.collate = None
        self.length = None
        self.comment = None
        self.nullable = True
        self.unsigned = None
        self.is_default_bit = False
        self.auto_increment = None

    def __str__(self):
        col_str = []
        col_str.append("NAME: {}".format(self.name))
        col_str.append("TYPE: {}".format(self.column_type))
        if self.is_default_bit:
            col_str.append("DEFAULT: b'{}'".format(self.default))
        else:
            col_str.append("DEFAULT: {}".format(self.default))
        col_str.append("LENGTH: {}".format(self.length))
        col_str.append("CHARSET: {}".format(self.charset))
        col_str.append("COLLATE: {}".format(self.collate))
        col_str.append("NULLABLE: {}".format(self.nullable))
        col_str.append("UNSIGNED: {}".format(self.unsigned))
        col_str.append("COMMENT: {}".format(self.comment))
        return ' '.join(col_str)

    @property
    def quoted_default(self):
        """
        Quote the default value if it's a numeric string. This is how MySQL
        does when you execute it without quotes
        """
        try:
            float(self.default)
            return "'{}'".format(self.default)
        except (ValueError, TypeError):
            return self.default

    def __eq__(self, other):
        for attr in (
                'name', 'column_type', 'charset', 'collate',
                'length', 'comment', 'nullable', 'unsigned', 'is_default_bit',
                'auto_increment'):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        # nullable column has implicit default as null
        if self.nullable:
            if self.quoted_default != other.quoted_default:
                # Implicit NULL equals to explicit default NULL
                # Other than that if there's any difference between two
                # default values they are semanticly different
                left_default_is_null = (
                    self.default is None or self.default.upper() == 'NULL')
                right_default_is_null = (
                    other.default is None or other.default.upper() == 'NULL')
                if not (left_default_is_null and right_default_is_null):
                    return False
        else:
            if self.quoted_default != other.quoted_default:
                return False

        return True

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        column_segment = []
        column_segment.append('`{}`'.format(escape(self.name)))
        if self.length is not None:
            column_segment.append('{}({})'
                                  .format(self.column_type, self.length))
        else:
            column_segment.append('{}'.format(self.column_type))

        if self.charset is not None:
            column_segment.append('CHARACTER SET {}'.format(self.charset))
        if self.unsigned is not None:
            column_segment.append('UNSIGNED')
        if self.collate is not None:
            column_segment.append('COLLATE {}'.format(self.collate))
        # By default MySQL will implicitly make column as nullable if not
        # specified
        if self.nullable or self.nullable is None:
            column_segment.append('NULL')
        else:
            column_segment.append('NOT NULL')
        if self.default is not None:
            if self.is_default_bit:
                column_segment.append('DEFAULT b{}'.format(self.default))
            else:
                column_segment.append('DEFAULT {}'.format(self.default))
        if self.auto_increment is not None:
            column_segment.append('AUTO_INCREMENT')
        if self.comment is not None:
            column_segment.append('COMMENT {}'.format(self.comment))

        return ' '.join(column_segment)


class TimestampColumn(Column):
    """
    A timestamp type column. It's different from other type of columns because
    it allow CURRENT_TIMESTAMP as a default value, and has a special attribute
    called "ON UPDATE"
    """
    def __init__(self):
        super(TimestampColumn, self).__init__()
        self.on_update_current_timestamp = None
        # We will not use the default nullable=True here, because timestamp
        # default behaviour is special
        self.nullable = None

    def __str__(self):
        col_str = super(TimestampColumn, self).__str__()
        col_str += ' ON UPDATE: {}'.format(self.on_update_current_timestamp)
        return col_str

    def explicit_ts_default(self):
        """"
        This is a special case for TimeStamp.
        If you define a column as
          `col` timestamp
        it has the exact the same meaning as
          `col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON
          UPDATE CURRENT_TIMESTAMP
        See also:
        http://dev.mysql.com/doc/refman/5.6/en/timestamp-initialization.html
        """
        if self.column_type == 'TIMESTAMP':
            if all([(self.nullable is None or not self.nullable),
                    self.default is None,
                    self.on_update_current_timestamp is None]):
                self.nullable = False
                self.default = 'CURRENT_TIMESTAMP'
                self.on_update_current_timestamp = 'CURRENT_TIMESTAMP'
        else:
            # Except timestamp, all other types have the implicit nullable
            # behavior.
            if self.nullable is None:
                self.nullable = True

    def __eq__(self, other):
        self.explicit_ts_default()
        if getattr(other, 'explicit_ts_default', None):
            other.explicit_ts_default()
        if not super(TimestampColumn, self).__eq__(other):
            return False
        for attr in ('on_update_current_timestamp',):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        self.explicit_ts_default()
        column_segment = []
        column_segment.append('`{}`'.format(escape(self.name)))
        if self.length is not None:
            column_segment.append('{}({})'
                                  .format(self.column_type, self.length))
        else:
            column_segment.append('{}'.format(self.column_type))
        if self.nullable:
            column_segment.append('NULL')
        else:
            column_segment.append('NOT NULL')
        if self.default is not None:
            column_segment.append('DEFAULT {}'.format(self.default))
        if self.on_update_current_timestamp is not None:
            column_segment.append('ON UPDATE {}'
                                  .format(self.on_update_current_timestamp))
        if self.comment is not None:
            column_segment.append('COMMENT {}'.format(self.comment))

        return ' '.join(column_segment)


class SetColumn(Column):
    """
    A set type column. It's different from other type of columns because it
    has a list of allowed values for definition
    """
    def __init__(self):
        super(SetColumn, self).__init__()
        self.set_list = []

    def __str__(self):
        col_str = super(SetColumn, self).__str__()
        col_str += ' SET VALUES: [{}]'.format(', '.join(self.set_list))
        return col_str

    def __eq__(self, other):
        if not super(SetColumn, self).__eq__(other):
            return False
        return self.set_list == other.set_list

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        column_segment = []
        column_segment.append('`{}`'.format(escape(self.name)))
        column_segment.append('{}({})'.format(
            self.column_type, ', '.join(self.set_list)
        ))
        if self.nullable:
            column_segment.append('NULL')
        else:
            column_segment.append('NOT NULL')
        if self.default is not None:
            column_segment.append('DEFAULT {}'.format(self.default))
        if self.comment is not None:
            column_segment.append('COMMENT {}'.format(self.comment))

        return ' '.join(column_segment)


class EnumColumn(Column):
    """
    A enum type column. It's different from other type of columns because it
    has a list of allowed values for definition
    """
    def __init__(self):
        super(EnumColumn, self).__init__()
        self.enum_list = []

    def __str__(self):
        col_str = super(EnumColumn, self).__str__()
        col_str += 'ENUM VALUES: [{}]'.format(', '.join(self.enum_list))
        return col_str

    def __eq__(self, other):
        if not super(EnumColumn, self).__eq__(other):
            return False
        return self.enum_list == other.enum_list

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        column_segment = []
        column_segment.append('`{}`'.format(escape(self.name)))
        column_segment.append('{}({})'.format(
            self.column_type, ', '.join(self.enum_list)
        ))
        if self.nullable:
            column_segment.append('NULL')
        else:
            column_segment.append('NOT NULL')
        if self.default is not None:
            column_segment.append('DEFAULT {}'.format(self.default))
        if self.comment is not None:
            column_segment.append('COMMENT {}'.format(self.comment))

        return ' '.join(column_segment)


class Table(object):
    """
    Representing a table definiton
    """
    def __init__(self):
        self.table_options = []
        self.name = None
        self.engine = None
        self.charset = None
        self.collate = None
        self.row_format = None
        self.key_block_size = None
        self.compression = None
        self.auto_increment = None
        self.comment = None
        self.column_list = []
        self.primary_key = TableIndex(name='PRIMARY', is_unique=True)
        self.indexes = []
        self.partition = None
        self.constraint = None

    def __str__(self):
        table_str = ''
        table_str += "NAME: {}\n".format(self.name)
        table_str += "ENGINE: {}\n".format(self.engine)
        table_str += "CHARSET: {}\n".format(self.charset)
        table_str += "COLLATE: {}\n".format(self.collate)
        table_str += "ROW_FORMAT: {}\n".format(self.row_format)
        table_str += "KEY_BLOCK_SIZE: {}\n".format(self.key_block_size)
        table_str += "COMPRESSION: {}\n".format(self.compression)
        table_str += "AUTO_INCREMENT: {}\n".format(self.auto_increment)
        table_str += "COMMENT: {}\n".format(self.comment)
        table_str += "PARTITION: {}\n".format(self.partition)
        for col in self.column_list:
            table_str += "[{}]\n".format(str(col))
        table_str += "PRIMARY_KEYS: \n"
        table_str += "\t{}\n".format(str(self.primary_key))
        table_str += "INDEXES: \n"
        for index in self.indexes:
            table_str += "\t{}\n".format(str(index))
        table_str += "Constraint: {}".format(str(self.constraint))

        return table_str

    def __eq__(self, other):
        for attr in (
                'name', 'engine', 'charset', 'collate', 'row_format',
                'key_block_size', 'comment', 'partition'):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        if self.primary_key != other.primary_key:
            return False
        for idx in self.indexes:
            if idx not in other.indexes:
                return False
        for idx in other.indexes:
            if idx not in self.indexes:
                return False
        if self.column_list != other.column_list:
            return False
        # If we get to this point, the two table structures are identical
        return True

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        """
        A standardize CREATE TABLE statement for creating the table
        """
        sql = "CREATE TABLE `{}` (\n".format(escape(self.name))

        col_strs = []
        for column in self.column_list:
            col_strs.append("    " + column.to_sql())
        sql += ",\n".join(col_strs)

        if self.primary_key.column_list:
            sql += ",\n    {}".format(self.primary_key.to_sql())

        if self.indexes:
            for idx in self.indexes:
                sql += ",\n    " + idx.to_sql()

        sql += "\n) "
        if self.engine is not None:
            sql += "ENGINE={} ".format(self.engine)
        if self.auto_increment is not None:
            sql += "AUTO_INCREMENT={} ".format(self.auto_increment)
        if self.charset is not None:
            sql += "DEFAULT CHARSET={} ".format(self.charset)
        if self.collate is not None:
            sql += "COLLATE={} ".format(self.collate)
        if self.row_format is not None:
            sql += "ROW_FORMAT={} ".format(self.row_format)
        if self.key_block_size is not None:
            sql += "KEY_BLOCK_SIZE={} ".format(self.key_block_size)
        if self.compression is not None:
            sql += "COMPRESSION={} ".format(self.compression)
        if self.comment is not None:
            sql += "COMMENT={} ".format(self.comment)
        if self.partition is not None:
            sql += "\n{} ".format(self.partition)
        return sql

    @property
    def checksum(self):
        """
        Generate a MD5 hash for the schema that this object stands for.
        In theory, two identical table shcema should have the exact same
        create table statement after standardize, and their MD5 hash should be
        the same as well.
        So you can tell whether two schema has the same structure by comparing
        their checksum value.
        """
        md5_obj = hashlib.md5(self.to_sql())
        return md5_obj.hexdigest()

    def droppable_indexes(self, keep_unique_key=False):
        """
        Drop index before loading, and create afterwards can make the whole
        process faster. Also the indexes will be more compact than loading
        directly.
        This function will return a list of droppable indexes for the
        purpose of fast index recreation.

        @param keep_unique_key: Keep unique key or not
        @type keep_unique_key: bool

        @return:  a list of droppable indexes to make load faster
        @rtype :  [TableIndex]
        """
        # Primary key should not be dropped, but it's not included in
        # table.indexes so we are fine here
        idx_list = []
        auto_incre_name = ''
        for col in self.column_list:
            if col.auto_increment:
                auto_incre_name = col.name
                break
        for idx in self.indexes:
            # Drop index which contains only the auto_increment column is
            # not allowed
            if len(idx.column_list) == 1:
                if auto_incre_name and \
                        auto_incre_name == idx.column_list[0].name:
                    continue
            # We can drop unique index for most of the time. Only if we want
            # to ignore duplicate key when adding new unique indexes, we need
            # to have the index exist on new table before loading data. So that
            # we can utilize "LOAD IGNORE" to ignore the duplicated data
            if keep_unique_key and idx.is_unique:
                continue
            idx_list.append(idx)
        return idx_list
