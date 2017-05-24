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


from .models import is_equal


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
        }
        for col in self.left.column_list:
            if col not in self.right.column_list:
                diffs['removed'].append(col)
        for col in self.right.column_list:
            if col not in self.left.column_list:
                diffs['added'].append(col)

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
            diff_str = "\n".join(diff_strs)
            return diff_str

    def diffs(self):
        return self._calculate_diff()
