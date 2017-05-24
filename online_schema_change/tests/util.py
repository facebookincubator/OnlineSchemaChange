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

import unittest
from ..lib.util import RangeChain


class RangeChainTest(unittest.TestCase):
    def test_range_chain_basic(self):
        chain = RangeChain()
        chain.extend([1, 2, 3, 4, 5, ])
        self.assertEqual(chain.missing_points(), [])

    def test_range_chain_with_gap(self):
        chain = RangeChain()
        chain.extend([1, 2, 3, 5, ])
        self.assertEqual(chain.missing_points(), [4])

    def test_range_chain_with_large_gap(self):
        chain = RangeChain()
        long_list = list(range(1, 20))
        points_to_remove = list(range(10, 15))
        for p in points_to_remove:
            long_list.remove(p)
        chain.extend(long_list)
        self.assertEqual(chain.missing_points(), points_to_remove)

    def test_append_with_gap(self):
        chain = RangeChain()
        chain.extend([1, 2, 3, 4, ])
        chain.extend([9, 10, 11, 12, ])
        self.assertEqual(chain.missing_points(), [5, 6, 7, 8])

    def test_extend_with_gap(self):
        chain = RangeChain()
        chain.extend([1, 2, 3, 4, ])
        chain.extend([9, 10, 12, ])
        self.assertEqual(chain.missing_points(), [5, 6, 7, 8, 11])

    def test_extend_with_empty_list(self):
        chain = RangeChain()
        chain.extend([1, 2, 3, 4, ])
        chain.extend([9, 10, 12, ])
        chain.extend([])
        self.assertEqual(chain.missing_points(), [5, 6, 7, 8, 11])

    def test_remove_point_from_gap(self):
        chain = RangeChain()
        chain.extend([1, 2, 3, 4, ])
        chain.extend([9, 10, 11, 12, ])
        chain.fill(6)
        self.assertEqual(chain.missing_points(), [5, 7, 8])

    def test_fill_existing_point(self):
        chain = RangeChain()
        chain.extend([1, 2, 3, 4, ])
        chain.extend([9, 10, 11, 12, ])
        with self.assertRaises(Exception):
            chain.fill(3)
