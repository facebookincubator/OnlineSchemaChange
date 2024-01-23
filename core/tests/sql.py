# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.


import unittest

from ..lib import sql


class SqlTestCase(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()

    def test_get_range_start_condition(self) -> None:
        with self.assertRaises(IndexError):
            sql.get_range_start_condition(["a"], [])

        self.assertEqual(sql.get_range_start_condition([], []), "")

        self.assertEqual(
            sql.get_range_start_condition(["a"], [1]),
            "( `a` > 1 )",
        )
        self.assertEqual(
            sql.get_range_start_condition(["a", "b"], [1, 2]),
            "( `a` > 1 ) OR ( `b` > 2 AND `a` = 1 )",
        )
        self.assertEqual(
            sql.get_range_start_condition(["a", "b", "c"], [1, 2, 3]),
            "( `a` > 1 ) OR ( `b` > 2 AND `a` = 1 ) OR ( `c` > 3 AND `a` = 1 AND `b` = 2 )",
        )
        self.assertEqual(
            sql.get_range_start_condition(["a", "b", "c"], ["x", 2, "z"]),
            "( `a` > x ) OR ( `b` > 2 AND `a` = x ) OR ( `c` > z AND `a` = x AND `b` = 2 )",
        )
