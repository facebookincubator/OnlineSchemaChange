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


from .models import is_equal, escape
import copy


class TableOptionDiff(object):
    def __init__(self, option_name, value):
        self.option_name = option_name
        self.value = value

    def to_sql(self):
        return "{}={}".format(self.option_name, self.value)


class SchemaDiff(object):
    """
    Representing the difference between two Table object
    """
    def __init__(self, left, right, ignore_partition=False):
        self.left = left
        self.right = right
        self.ignore_partition = ignore_partition

    def _calculate_diff(self):
        diffs = {
            'removed': [],
            'added': [],
            # Customized messages
            'msgs': []
        }
        # We are copying here since we want to change the col list.
        # Shallow copy should be enough here
        col_left_copy = copy.copy(self.left.column_list)
        col_right_copy = copy.copy(self.right.column_list)
        for col in self.left.column_list:
            if col not in self.right.column_list:
                diffs['removed'].append(col)
                col_left_copy.remove(col)

        for col in self.right.column_list:
            if col not in self.left.column_list:
                diffs['added'].append(col)
                col_right_copy.remove(col)

        # Two tables have different col order
        if sorted(col_left_copy, key=lambda col: col.name) == sorted(
                col_right_copy, key=lambda col: col.name):
            old_order = []
            new_order = []
            for col1, col2 in zip(col_left_copy, col_right_copy):
                if col1 != col2:
                    old_order.append(col1.name)
                    new_order.append(col2.name)
            if old_order:
                diffs["msgs"].append("Column order mismatch was detected:")
                diffs["msgs"].append("- " + ", ".join(old_order))
                diffs["msgs"].append("+ " + ", ".join(new_order))

        for idx in self.left.indexes:
            if idx not in self.right.indexes:
                diffs['removed'].append(idx)
        for idx in self.right.indexes:
            if idx not in self.left.indexes:
                diffs['added'].append(idx)

        if self.left.primary_key != self.right.primary_key:
            if self.left.primary_key.column_list:
                diffs['removed'].append(self.left.primary_key)
            if self.right.primary_key.column_list:
                diffs['added'].append(self.right.primary_key)

        attrs_to_check = [
            'name', 'engine', 'charset', 'collate', 'row_format',
            'key_block_size', 'comment']
        if not self.ignore_partition:
            attrs_to_check.append('partition')

        for attr in attrs_to_check:
            tbl_option_old = getattr(self.left, attr)
            tbl_option_new = getattr(self.right, attr)
            if not is_equal(tbl_option_old, tbl_option_new):
                diffs['removed'].append(TableOptionDiff(attr, tbl_option_old))
                diffs['added'].append(TableOptionDiff(attr, tbl_option_new))

        return diffs

    def __str__(self):
        if self.left == self.right:
            return "No difference"
        else:
            diff_strs = []
            diffs = self._calculate_diff()
            for diff in diffs['removed']:
                diff_strs.append('- ' + diff.to_sql())
            for diff in diffs['added']:
                diff_strs.append('+ ' + diff.to_sql())
            for diff in diffs["msgs"]:
                diff_strs.append(diff)
            diff_str = "\n".join(diff_strs)
            return diff_str

    def diffs(self):
        return self._calculate_diff()

    def _gen_col_sql(self):
        """
        Generate the column section for ALTER TABLE statement
        """
        segments = []
        left_column_names = {col.name: col for col in self.left.column_list}
        right_column_names = {col.name: col for col in self.right.column_list}

        # Drop columns
        for col in self.left.column_list:
            if col.name not in right_column_names.keys():
                segments.append("DROP `{}`".format(escape(col.name)))

        # Add columns
        for idx, col in enumerate(self.right.column_list):
            if col.name not in left_column_names.keys():
                if idx == 0:
                    position = 'FIRST'
                else:
                    position = 'AFTER `{}`'.format(escape(
                        self.right.column_list[idx - 1].name))
                segments.append("ADD {} {}".format(col.to_sql(), position))

        # Modify columns
        for col in self.right.column_list:
            if col.name in left_column_names and col != left_column_names[col.name]:
                segments.append("MODIFY {}".format(col.to_sql()))

        return segments

    def _gen_idx_sql(self):
        """
        Generate the index section for ALTER TABLE statement
        """
        segments = []
        # Drop index
        for idx in self.left.indexes:
            if idx not in self.right.indexes:
                segments.append("DROP KEY `{}`".format(escape(idx.name)))

        # Add index
        for idx in self.right.indexes:
            if idx not in self.left.indexes:
                segments.append("ADD {}".format(idx.to_sql()))

        if self.left.primary_key and not self.right.primary_key:
            segments.append("DROP PRIMARY KEY")
        elif not self.left.primary_key.column_list \
                and self.right.primary_key.column_list:
            segments.append("ADD {}".format(self.right.primary_key.to_sql()))
        elif self.left.primary_key != self.right.primary_key:
            segments.append("DROP PRIMARY KEY")
            segments.append("ADD {}".format(self.right.primary_key.to_sql()))

        return segments

    def _gen_tbl_attr_sql(self):
        """
        Generate the table attribute section for ALTER TABLE statement
        """
        segments = []
        attrs_to_check = [
            'charset', 'collate', 'row_format', 'key_block_size', 'comment'
        ]
        if not self.ignore_partition:
            attrs_to_check.append('partition')

        for attr in attrs_to_check:
            tbl_option_old = getattr(self.left, attr)
            tbl_option_new = getattr(self.right, attr)
            if not is_equal(tbl_option_old, tbl_option_new):
                segments.append("{}={}".format(attr, tbl_option_new))

        return segments

    def to_sql(self):
        """
        Generate an ALTER TABLE statement that can bring the schema from left to
        right
        """
        segments = []

        segments.extend(self._gen_col_sql())
        segments.extend(self._gen_idx_sql())
        segments.extend(self._gen_tbl_attr_sql())

        if segments:
            return "ALTER TABLE `{}` {}".format(
                escape(self.right.name), ", ".join(segments))


def get_type_conv_columns(old_obj, new_obj):
    """
    Return a list of columns that involve type conversion when transit from left to
    right
    """
    type_conv_cols = []

    current_cols = {c.name: c for c in old_obj.column_list}
    new_cols = {c.name: c for c in new_obj.column_list}

    # find columns that will involve type conversions
    for name, old_col in current_cols.items():
        new_col = new_cols.get(name)

        # this column isn't in the new schema, so it
        # doesn't matter
        if new_col is None:
            continue

        # Type changes are considered as type conversion
        if new_col.column_type != old_col.column_type:
            type_conv_cols.append(old_col)
        else:
            # Length change also considered as type conversion
            if new_col.length != old_col.length:
                type_conv_cols.append(old_col)
    return type_conv_cols
