# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-ignore-all-errors


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

    def test_get_match_clause(self) -> None:
        clause = sql.get_match_clause(
            "__osc_new_tbl",
            "__osc_chg_tbl",
            ["col1", "col2"],
            " AND ",
            {"col1": "latin1"},
        )

        self.assertEqual(
            clause,
            "`__osc_new_tbl`.`col1` = CONVERT(`__osc_chg_tbl`.`col1` using `latin1`) AND `__osc_new_tbl`.`col2` = `__osc_chg_tbl`.`col2`",
        )

        clause = sql.get_match_clause(
            "__osc_new_tbl",
            "__osc_chg_tbl",
            ["col1", "col2"],
            " AND ",
            {"col1": "latin1", "col2": None},
        )

        self.assertEqual(
            clause,
            "`__osc_new_tbl`.`col1` = CONVERT(`__osc_chg_tbl`.`col1` using `latin1`) AND `__osc_new_tbl`.`col2` = `__osc_chg_tbl`.`col2`",
        )

        clause = sql.get_match_clause(
            "__osc_new_tbl",
            "__osc_chg_tbl",
            ["col1", "col2"],
            " AND ",
            {"col1": "latin1", "col2": "utf8"},
        )

        self.assertEqual(
            clause,
            "`__osc_new_tbl`.`col1` = CONVERT(`__osc_chg_tbl`.`col1` using `latin1`) AND `__osc_new_tbl`.`col2` = CONVERT(`__osc_chg_tbl`.`col2` using `utf8`)",
        )

        clause = sql.get_match_clause(
            "__osc_new_tbl",
            "__osc_chg_tbl",
            ["col1", "col2"],
            " AND ",
            {},
        )

        self.assertEqual(
            clause,
            "`__osc_new_tbl`.`col1` = `__osc_chg_tbl`.`col1` AND `__osc_new_tbl`.`col2` = `__osc_chg_tbl`.`col2`",
        )

    def test_get_delete_row(self) -> None:
        clause = sql.replay_delete_row(
            "__osc_new_tbl",
            "__osc_chg_tbl",
            "_osc_ID",
            ["col1", "col2"],
            {"col1": "latin1"},
        )

        self.assertEqual(
            clause,
            "DELETE __osc_new_tbl FROM `__osc_new_tbl`, `__osc_chg_tbl` WHERE `__osc_chg_tbl`.`_osc_ID` IN %s AND `__osc_new_tbl`.`col1` = CONVERT(`__osc_chg_tbl`.`col1` using `latin1`) AND `__osc_new_tbl`.`col2` = `__osc_chg_tbl`.`col2`",
        )
