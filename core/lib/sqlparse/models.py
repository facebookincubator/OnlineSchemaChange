"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import hashlib
import logging
import re
from typing import List, NamedTuple, Optional, Set, Union

log = logging.getLogger(__name__)

IGNORE_TABLESPACES = ["innodb_system"]


def escape(keyword):
    """
    Escape the backtick for keyword when generating an actual SQL.
    """
    return keyword.replace("`", "``")


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


class IndexColumn:
    """
    A column definition inside index section.
    This is different from a table column definition, because only `name`,
    `length`, `order` are required for a index column definition
    """

    def __init__(self):
        self.name = None
        self.length = None
        self.order = "ASC"

    def __str__(self):
        str_repr = ""
        if self.length is not None:
            str_repr = "{}({})".format(self.name, self.length)
        else:
            str_repr = "{}".format(self.name)
        if self.order != "ASC":
            str_repr += " DESC"
        return str_repr

    def __eq__(self, other):
        if self.name != other.name:
            return False
        return self.length == other.length and self.order == other.order

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        sql_str = ""
        if self.length is not None:
            sql_str = "`{}`({})".format(escape(self.name), self.length)
        else:
            sql_str = "`{}`".format(escape(self.name))
        if self.order != "ASC":
            sql_str += " DESC"
        return sql_str


class DocStoreIndexColumn:
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
            return "{} AS {}({})".format(self.document_path, self.key_type, self.length)
        else:
            return "{} AS {}".format(self.document_path, self.key_type)

    def __eq__(self, other):
        for attr in ("document_path", "key_type", "length"):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        if self.length is not None:
            return "{} AS {}({})".format(self.document_path, self.key_type, self.length)
        else:
            return "{} AS {}".format(self.document_path, self.key_type)


class TableIndex:
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
        self.visibility = True

    def __str__(self):
        idx_str = []
        idx_str.append("NAME: {}".format(self.name))
        idx_str.append("IS UNIQUE: {}".format(self.is_unique))
        idx_str.append("TYPE: {}".format(self.key_type))
        col_list_str = []
        for col_str in self.column_list:
            col_list_str.append(str(col_str))
        idx_str.append("KEY LIST: {}".format(",".join(col_list_str)))
        if self.using:
            idx_str.append("USING: {}".format(self.using))
        idx_str.append("KEY_BLOCK_SIZE: {}".format(self.key_block_size))
        idx_str.append("COMMENT: {}".format(self.comment))
        return "/ ".join(idx_str)

    def __eq__(self, other):
        for attr in (
            "name",
            "key_block_size",
            "comment",
            "is_unique",
            "key_type",
            "using",
            "visibility",
        ):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        return self.column_list == other.column_list

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        segments = []
        if self.name is not None:
            if self.name == "PRIMARY":
                segments.append("PRIMARY KEY")
            else:
                if self.is_unique:
                    segments.append("UNIQUE KEY `{}`".format(escape(self.name)))
                elif self.key_type is not None:
                    segments.append(
                        "{} KEY `{}`".format(self.key_type, escape(self.name))
                    )
                else:
                    segments.append("KEY `{}`".format(escape(self.name)))
        else:
            segments.append("KEY")

        segments.append(
            "({})".format(", ".join([col.to_sql() for col in self.column_list]))
        )
        if self.using is not None:
            segments.append("USING {}".format(self.using))
        if self.key_block_size is not None:
            segments.append("KEY_BLOCK_SIZE={}".format(self.key_block_size))
        if self.comment is not None:
            segments.append("COMMENT {}".format(self.comment))
        if not self.visibility:
            segments.append("INVISIBLE")
        return " ".join(segments)


class Column:
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
        self.zerofill = None
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
        col_str.append("ZEROFILL: {}".format(self.zerofill))
        col_str.append("UNSIGNED: {}".format(self.unsigned))
        col_str.append("COMMENT: {}".format(self.comment))
        return " ".join(col_str)

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
            "name",
            "column_type",
            "charset",
            "collate",
            "length",
            "comment",
            "nullable",
            "zerofill",
            "unsigned",
            "is_default_bit",
            "auto_increment",
        ):

            # Ignore display width of *int types, because of the new default in 8.0.20.
            # This is a bit of a heavy hammer, but it's the simpler alternative to be
            # able to support mixed version comparisons
            # Ref: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-19.html
            # (search for: "Display width specification for integer data types")
            int_types = {"int", "bigint", "tinyint", "smallint", "mediumint"}
            if self.column_type.lower() in int_types and attr == "length":
                continue

            if (
                attr == "column_type"
                and getattr(self, attr) in ("BOOL", "BOOLEAN")
                and getattr(other, attr) in ("BOOL", "BOOLEAN")
            ):
                self.column_type = "BOOLEAN"
                continue

            # "utf8" and "utf8mb3" are alias for column charset
            # Ref: https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-utf8mb3.html
            if attr == "charset":
                if getattr(self, attr) in ("utf8", "utf8mb3") and getattr(
                    other, attr
                ) in ("utf8", "utf8mb3"):
                    continue

            if attr == "collate":
                cur_attr = getattr(self, attr)
                other_attr = getattr(other, attr)

                if cur_attr and other_attr:
                    cur_attr = cur_attr.replace("utf8mb3", "utf8")
                    other_attr = other_attr.replace("utf8mb3", "utf8")
                    if cur_attr == other_attr:
                        continue

            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        return self.has_same_default(other)

    def has_same_default(self, other):
        # nullable column has implicit default as null
        if self.nullable:
            if self.quoted_default != other.quoted_default:
                # Implicit NULL equals to explicit default NULL
                # Other than that if there's any difference between two
                # default values they are semanticly different
                left_default_is_null = (
                    self.default is None or self.default.upper() == "NULL"
                )
                right_default_is_null = (
                    other.default is None or other.default.upper() == "NULL"
                )
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
        column_segment.append("`{}`".format(escape(self.name)))
        if self.length is not None:
            column_segment.append("{}({})".format(self.column_type, self.length))
        else:
            column_segment.append("{}".format(self.column_type))

        if self.charset is not None:
            column_segment.append("CHARACTER SET {}".format(self.charset))
        if self.unsigned is not None:
            column_segment.append("UNSIGNED")
        if self.zerofill is not None:
            column_segment.append("ZEROFILL")
        if self.collate is not None:
            column_segment.append("COLLATE {}".format(self.collate))
        # By default MySQL will implicitly make column as nullable if not
        # specified
        if self.nullable or self.nullable is None:
            column_segment.append("NULL")
        else:
            column_segment.append("NOT NULL")
        if self.default is not None:
            if self.is_default_bit:
                column_segment.append("DEFAULT b{}".format(self.default))
            else:
                column_segment.append("DEFAULT {}".format(self.default))
        if self.auto_increment is not None:
            column_segment.append("AUTO_INCREMENT")
        if self.comment is not None:
            column_segment.append("COMMENT {}".format(self.comment))

        return " ".join(column_segment)


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
        col_str += " ON UPDATE: {}".format(self.on_update_current_timestamp)
        return col_str

    def explicit_ts_default(self):
        """ "
        This is a special case for TimeStamp.
        If you define a column as
          `col` timestamp
        it has the exact the same meaning as
          `col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON
          UPDATE CURRENT_TIMESTAMP
        See also:
        http://dev.mysql.com/doc/refman/5.6/en/timestamp-initialization.html
        """
        if self.column_type == "TIMESTAMP":
            if all(
                [
                    (self.nullable is None or not self.nullable),
                    self.default is None,
                    self.on_update_current_timestamp is None,
                ]
            ):
                self.nullable = False
                self.default = "CURRENT_TIMESTAMP"
                self.on_update_current_timestamp = "CURRENT_TIMESTAMP"
        else:
            # Except timestamp, all other types have the implicit nullable
            # behavior.
            if self.nullable is None:
                self.nullable = True

    def __eq__(self, other):
        self.explicit_ts_default()
        if getattr(other, "explicit_ts_default", None):
            other.explicit_ts_default()
        if not super(TimestampColumn, self).__eq__(other):
            return False
        for attr in ("on_update_current_timestamp",):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        self.explicit_ts_default()
        column_segment = []
        column_segment.append("`{}`".format(escape(self.name)))
        if self.length is not None:
            column_segment.append("{}({})".format(self.column_type, self.length))
        else:
            column_segment.append("{}".format(self.column_type))
        if self.nullable:
            column_segment.append("NULL")
        else:
            column_segment.append("NOT NULL")
        if self.default is not None:
            column_segment.append("DEFAULT {}".format(self.default))
        if self.on_update_current_timestamp is not None:
            column_segment.append(
                "ON UPDATE {}".format(self.on_update_current_timestamp)
            )
        if self.comment is not None:
            column_segment.append("COMMENT {}".format(self.comment))

        return " ".join(column_segment)


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
        col_str += " SET VALUES: [{}]".format(", ".join(self.set_list))
        return col_str

    def __eq__(self, other):
        if not super(SetColumn, self).__eq__(other):
            return False
        return self.set_list == other.set_list

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        column_segment = []
        column_segment.append("`{}`".format(escape(self.name)))
        column_segment.append(
            "{}({})".format(self.column_type, ", ".join(self.set_list))
        )
        if self.nullable:
            column_segment.append("NULL")
        else:
            column_segment.append("NOT NULL")
        if self.default is not None:
            column_segment.append("DEFAULT {}".format(self.default))
        if self.comment is not None:
            column_segment.append("COMMENT {}".format(self.comment))

        return " ".join(column_segment)


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
        col_str += "ENUM VALUES: [{}]".format(", ".join(self.enum_list))
        return col_str

    def __eq__(self, other):
        if not super(EnumColumn, self).__eq__(other):
            return False
        return self.enum_list == other.enum_list

    def __ne__(self, other):
        return not self == other

    def to_sql(self):
        column_segment = []
        column_segment.append("`{}`".format(escape(self.name)))
        column_segment.append(
            "{}({})".format(self.column_type, ", ".join(self.enum_list))
        )
        if self.charset is not None:
            column_segment.append("CHARACTER SET {}".format(self.charset))
        if self.collate is not None:
            column_segment.append("COLLATE {}".format(self.collate))
        if self.nullable:
            column_segment.append("NULL")
        else:
            column_segment.append("NOT NULL")
        if self.default is not None:
            column_segment.append("DEFAULT {}".format(self.default))
        if self.comment is not None:
            column_segment.append("COMMENT {}".format(self.comment))

        return " ".join(column_segment)


class PartitionDefinitionEntry(NamedTuple):
    pdef_name: str
    pdef_type: str
    pdef_value_list: Union[List[str], str]
    pdef_comment: Optional[str]
    pdef_engine: str = "INNODB"
    is_tuple: bool = False


class PartitionConfig:
    # Partitions config for a table
    PTYPE_RANGE = "RANGE"
    PTYPE_LIST = "LIST"
    PTYPE_HASH = "HASH"
    PTYPE_KEY = "KEY"

    SUBTYPE_L = "LINEAR"
    SUBTYPE_C = "COLUMNS"

    KNOWN_PARTITION_TYPES: Set[str] = {PTYPE_LIST, PTYPE_HASH, PTYPE_KEY, PTYPE_RANGE}
    KNOWN_PARTITION_SUBTYPES: Set[str] = {SUBTYPE_L, SUBTYPE_C}

    PDEF_TYPE_VIN = "p_values_in"
    PDEF_TYPE_VLT = "p_values_less_than"
    PDEF_TYPE_ATTRIBS: List[str] = [PDEF_TYPE_VIN, PDEF_TYPE_VLT]
    TYPE_MAP = {
        PDEF_TYPE_VIN: "IN",
        PDEF_TYPE_VLT: "LESS THAN",
    }

    def __init__(self) -> None:
        self.part_type: Optional[str] = None  # Partition type e.g. RANGE
        self.p_subtype: Optional[str] = None  # e.g. LINEAR / COLUMNS
        self.num_partitions: int = 0
        self.fields_or_expr: Optional[Union[str, List[str]]] = None

        self.part_defs: List[PartitionDefinitionEntry] = []
        self.full_type: str = ""
        # Partition type `KEY` alone allows specifying ALGORITHM=[1|2] e.g.
        # `PARTITION BY linear key ALGORITHM=2 (id)  partitions 10`
        self.algorithm_for_key: Optional[int] = None
        self.via_nested_expr = False

    def __str__(self):
        return (
            f"{self.__class__.__name__}: |"
            f"type={self.full_type}|"
            f"fields_or_expr={self.fields_or_expr}|"
            f"defs={self.part_defs}|numparts={self.num_partitions}"
        )

    def get_type(self) -> Optional[str]:
        return self.full_type

    def get_num_parts(self) -> int:
        return self.num_partitions

    def get_fields_or_expr(self) -> Optional[Union[str, List[str]]]:
        return self.fields_or_expr

    def get_algo(self) -> Optional[int]:
        return self.algorithm_for_key if self.part_type == self.PTYPE_KEY else None

    def __eq__(self, other):
        for attr in (
            "part_type",
            "p_subtype",
            "num_partitions",
            "fields_or_expr",
            "full_type",
            "algorithm_for_key",
        ):
            if not is_equal(getattr(self, attr), getattr(other, attr)):
                return False

        return self.part_defs == other.part_defs

    def __ne__(self, other):
        return not self == other

    def add_quote(self, field: str) -> str:
        return f"`{field}`"

    def to_partial_sql(self):
        # Stringify info a format usable in `create table ...`

        def _proc_list(vals: Union[str, List[str]]) -> str:
            # Helper to convert expr list to an expression value-list
            if isinstance(vals, list) and all(isinstance(v, str) for v in vals):
                return "(" + ", ".join(vals) + ")"
            ret = ""
            for v in vals:
                if isinstance(v, list):
                    ret += _proc_list(v)
                else:
                    ret += v
            return ret

        output = f"PARTITION BY {self.full_type}"

        if self.part_type == self.PTYPE_KEY:
            if self.algorithm_for_key is not None:
                output += f" ALGORITHM={self.algorithm_for_key}"
            fields = ", ".join(self.add_quote(f) for f in self.fields_or_expr)
            output += f" ({fields})"
            if self.num_partitions > 1:
                output += f" PARTITIONS {self.num_partitions}"
            return output
        elif self.part_type == self.PTYPE_HASH:
            output += (
                f" ({_proc_list(self.fields_or_expr)})"
                if any(isinstance(x, list) for x in self.fields_or_expr)
                else f" {_proc_list(self.fields_or_expr)}"
            )
            if self.num_partitions > 1:
                output += f" PARTITIONS {self.num_partitions}"
            return output
        elif self.part_type == self.PTYPE_RANGE or self.part_type == self.PTYPE_LIST:
            partitions: List[str] = []
            for pd in self.part_defs:
                name = f"`{pd.pdef_name}`" if pd.pdef_name.isdigit() else pd.pdef_name
                ty = self.TYPE_MAP[pd.pdef_type]
                expr_or_value_list = (
                    _proc_list(pd.pdef_value_list)
                    if isinstance(pd.pdef_value_list, list)
                    else pd.pdef_value_list
                )
                eng = pd.pdef_engine
                if pd.is_tuple:
                    expr_or_value_list = f"({expr_or_value_list})"
                thispart = (
                    f"PARTITION {name} VALUES {ty} {expr_or_value_list} ENGINE {eng}"
                )
                comment = pd.pdef_comment
                if comment is not None:
                    thispart += f" COMMENT {comment}"
                partitions.append(thispart)
            f_or_e = _proc_list(self.fields_or_expr)
            if self.via_nested_expr:
                # PART_EXPR in sqlparse use nestedExpr to acquire this
                # and strips parens so "undo" that
                f_or_e = f"({f_or_e})"
            output += f" {f_or_e} (\n" + ",\n".join(partitions) + ")"
            return output


class Table:
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
        self.primary_key = TableIndex(name="PRIMARY", is_unique=True)
        self.indexes = []
        self.partition = None  # Partitions as a string
        self.constraint = None
        self.partition_config: Optional[PartitionConfig] = None
        self.has_80_features = False
        self.tablespace = None
        self.fk_constraint = {}

    def __str__(self):
        table_str = ""
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
        table_str += "CONSTRAINT: {}\n".format(str(self.constraint))
        table_str += "TABLESPACE: {}".format(str(self.tablespace))

        return table_str

    def __eq__(self, other):
        for attr in (
            "name",
            "engine",
            "charset",
            "collate",
            "row_format",
            "key_block_size",
            "comment",
            # "partition",
            "partition_config",
            "constraint",
        ):
            # "utf8" and "utf8mb3" are alias for table charset
            # Ref: https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-utf8mb3.html
            if attr == "charset":
                if getattr(self, attr) in ("utf8", "utf8mb3") and getattr(
                    other, attr
                ) in ("utf8", "utf8mb3"):
                    continue

            if attr == "row_format":
                engineCheck = "engine"
                currentEngine = getattr(self, engineCheck)
                otherEngine = getattr(other, engineCheck)
                if currentEngine and otherEngine:
                    currentEngine = currentEngine.upper()
                    otherEngine = otherEngine.upper()

                    # MyRocks has only one row format.
                    if currentEngine == otherEngine and otherEngine == "ROCKSDB":
                        continue

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
        if self.tablespace is not None:
            # ignore innodb_system tablespace
            if self.tablespace not in IGNORE_TABLESPACES:
                sql += "/*!50100 TABLESPACE `{}` */".format(self.tablespace)
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
        md5_obj = hashlib.md5(self.to_sql().encode("utf-8"))
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
        auto_incre_name = ""
        for col in self.column_list:
            if col.auto_increment:
                auto_incre_name = col.name
                break
        for idx in self.indexes:
            # Drop index which contains only the auto_increment column is
            # not allowed
            if len(idx.column_list) == 1:
                if auto_incre_name and auto_incre_name == idx.column_list[0].name:
                    continue
            # We can drop unique index for most of the time. Only if we want
            # to ignore duplicate key when adding new unique indexes, we need
            # to have the index exist on new table before loading data. So that
            # we can utilize "LOAD IGNORE" to ignore the duplicated data
            if keep_unique_key and idx.is_unique:
                continue
            idx_list.append(idx)
        return idx_list

    @property
    def is_myrocks_ttl_table(self):
        if not self.engine:
            return False
        if self.engine.upper() == "ROCKSDB":
            if self.comment:
                # partition level ttl
                if re.search(r"\S+_ttl_duration=[0-9]+;", self.comment):
                    return True
                # table level ttl
                elif re.search(r"ttl_duration=[0-9]+;", self.comment):
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False
