#!/usr/bin/env python3
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

import logging
import unittest

from osc.lib.sqlparse import CreateParser, PartitionParseError
from osc.lib.sqlparse.models import PartitionDefinitionEntry, PartitionConfig

log = logging.getLogger(__name__)

PARTS_RANGE_WITH_OPTS = (
    "PARTITION BY RANGE (store_id) ("
    "PARTITION p0 VALUES LESS THAN (6) ENGINE = 'innodb' COMMENT 'whatever',"
    "PARTITION p1 VALUES LESS THAN (11),"
    "PARTITION p2 VALUES LESS THAN (16),"
    "PARTITION p3 VALUES LESS THAN (21),"
    "PARTITION p4 VALUES LESS THAN maxvalue"
    ")"
)

PARTS_RANGE_WITH_OPTS_DUPE = (
    "PARTITION BY RANGE (store_id) ("
    "PARTITION p0 VALUES LESS THAN (6) COMMENT 'whatever',"
    "PARTITION p1 VALUES LESS THAN (11) ENGINE = InnoDB,"
    "PARTITION p2 VALUES LESS THAN (16) ENGINE = 'innodb',"
    "PARTITION p3 VALUES LESS THAN (21) ENGINE = InnoDB,"
    "PARTITION p4 VALUES LESS THAN maxvalue"
    ")"
)

# Uses a formula/expr rather than plain col names
PARTS_RANGE_WITH_EXPR = (
    "PARTITION BY RANGE ( UNIX_TIMESTAMP(a))"
    "(PARTITION p1 VALUES LESS THAN (1554015600) ENGINE = InnoDB,"
    " PARTITION p2 VALUES LESS THAN (1558249200) ENGINE = InnoDB)"
)

PARTS_RANGE_WITH_COLUMNS = (
    "PARTITION BY RANGE COLUMNS (renewal) ("
    "PARTITION pWeek_1 VALUES LESS THAN('2010-02-09'),"
    "PARTITION pWeek_2 VALUES LESS THAN('2010-02-15'),"
    "PARTITION pWeek_3 VALUES LESS THAN('2010-02-22'),"
    "PARTITION pWeek_4 VALUES LESS THAN('2010-03-01')"
    ")"
)

PARTS_LIST_IN = (
    "PARTITION BY LIST (`store_id`) ("
    "PARTITION pNorth VALUES IN (3,5,6,9,17),"
    "PARTITION pEast VALUES IN (1,2,10,11,19,20),"
    "PARTITION pWest VALUES IN (4,12,13,14,18),"
    "PARTITION pCentral VALUES IN (7,8,15,16)"
    ")"
)

PARTS_LIST_IN_WITH_COLUMNS = (
    "PARTITION BY LIST COLUMNS (city) ("
    "PARTITION pRegion_1 VALUES IN('Oskarshamn', 'Högsby', 'Mönsterås'),"
    "PARTITION pRegion_2 VALUES IN('Vimmerby', 'Hultsfred', 'Västervik'),"
    "PARTITION pRegion_3 VALUES IN('Nässjö', 'Eksjö', 'Vetlanda'),"
    "PARTITION pRegion_4 VALUES IN('Uppvidinge', 'Alvesta', 'Vaxjo')"
    ")"
)

PARTS_LIST_IN_WITH_COLUMNS_INTVALS = (
    "PARTITION BY LIST COLUMNS (someid) ("
    "PARTITION pRegion_1 VALUES IN(1, 5, 9, 13),"
    "PARTITION pRegion_2 VALUES IN(2, 6, 10, 14),"
    "PARTITION pRegion_3 VALUES IN(3, 7, 11, 15),"
    "PARTITION pRegion_4 VALUES IN(4, 8, 12, 16)"
    ")"
)

PARTS_KEY_NO_PARTCOUNT = "PARTITION BY key()"
PARTS_KEY_EMPTY = "PARTITION BY key() PARTITIONS 2"
PARTS_KEY_NONEMPTY = "PARTITION BY KEY(id1, `id2`) PARTITIONS 2"
PARTS_KEY_LINEAR_ALGO = "PARTITION BY linear key ALGORITHM=2 (id)  partitions 10"

# Parsing the `expr` for hash is ugly.
PARTS_HASH = "PARTITION BY HASH (YEAR(hired)) PARTITIONS 3"
PARTS_HASH_WITH_LINEAR = "PARTITION BY linear hash(YEAR(hired)) PARTITIONS 4"

PARTS_RANGE_MANY_ENGINES = (
    "PARTITION BY RANGE (store_id) ("
    "PARTITION p0 VALUES LESS THAN (6) ENGINE = 'innodb' COMMENT 'whatever',"
    "PARTITION p1 VALUES LESS THAN (11) ENGINE = 'rocksdb',"
    "PARTITION p2 VALUES LESS THAN (16),"
    "PARTITION p3 VALUES LESS THAN (21),"
    "PARTITION p4 VALUES LESS THAN maxvalue"
    ")"
)


# Test parsing partitions config (alone)
class PartitionParserTest(unittest.TestCase):
    # See https://www.internalfb.com/phabricator/paste/view/P448439189?lines=675
    # for dump of raw ParseResults
    def test_parts_range(self):
        result = CreateParser.parse_partitions(PARTS_RANGE_WITH_OPTS)
        log.error(f"test_parts_range1 Res: {result.dump()}")

        self.assertEqual("RANGE", result.part_type)
        self.assertEqual(5, len(result.part_defs))  # 5 partitions defined
        self.assertEqual(["store_id"], result.p_expr.asList())

        # Check p0
        p0 = result.part_defs[0]
        self.assertEqual("p0", p0.part_name)
        self.assertEqual("INNODB", p0.pdef_engine)
        self.assertEqual("'whatever'", p0.pdef_comment)
        self.assertEqual([["6"]], p0.p_values_less_than.asList())
        # Check p4
        # asList for str vs (num1) behaves differently :-()
        self.assertEqual(["MAXVALUE"], result.part_defs[4].p_values_less_than.asList())

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_range_model1 Model {pc}")
        self.assertEqual("RANGE", pc.get_type())
        self.assertEqual(5, pc.get_num_parts())
        self.assertEqual(["store_id"], pc.get_fields_or_expr())
        entries = [
            PartitionDefinitionEntry(
                pdef_name="p0",
                pdef_type="p_values_less_than",
                pdef_value_list=["6"],
                pdef_comment="'whatever'",
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="p1",
                pdef_type="p_values_less_than",
                pdef_value_list=["11"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="p2",
                pdef_type="p_values_less_than",
                pdef_value_list=["16"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="p3",
                pdef_type="p_values_less_than",
                pdef_value_list=["21"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="p4",
                pdef_type="p_values_less_than",
                pdef_value_list="MAXVALUE",
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
        ]
        self.assertEqual(entries, pc.part_defs)

    def test_parts_range_cols(self):
        result = CreateParser.parse_partitions(PARTS_RANGE_WITH_COLUMNS)
        log.error(f"test_parts_range_cols2 Res: {result.dump()}")
        self.assertEqual("RANGE", result.part_type)
        self.assertEqual("COLUMNS", result.p_subtype)
        self.assertEqual(4, len(result.part_defs))
        pweek_4 = result.part_defs[3]
        self.assertEqual("pWeek_4", pweek_4.part_name)
        self.assertEqual(["renewal"], result.field_list.asList())
        self.assertEqual([["'2010-03-01'"]], pweek_4.p_values_less_than.asList())

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_range_cols2 Model {pc}")
        self.assertEqual("RANGE COLUMNS", pc.get_type())
        self.assertEqual(4, pc.get_num_parts())
        self.assertEqual(["renewal"], pc.get_fields_or_expr())
        entries = [
            PartitionDefinitionEntry(
                pdef_name="pWeek_1",
                pdef_type="p_values_less_than",
                pdef_value_list=["'2010-02-09'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pWeek_2",
                pdef_type="p_values_less_than",
                pdef_value_list=["'2010-02-15'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pWeek_3",
                pdef_type="p_values_less_than",
                pdef_value_list=["'2010-02-22'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pWeek_4",
                pdef_type="p_values_less_than",
                pdef_value_list=["'2010-03-01'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
        ]
        self.assertEqual(entries, pc.part_defs)

    def test_parts_list(self):
        result = CreateParser.parse_partitions(PARTS_LIST_IN)
        log.error(f"test_parts_list3 Res: {result.dump()}")
        self.assertEqual("LIST", result.part_type)
        self.assertEqual(4, len(result.part_defs))
        self.assertEqual(["store_id"], result.p_expr.asList())

        # Check p0
        pNorth = result.part_defs[0]
        self.assertEqual("pNorth", pNorth.part_name)
        self.assertEqual([["3", "5", "6", "9", "17"]], pNorth.p_values_in.asList())

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_list3 Model {pc}")
        self.assertEqual("LIST", pc.get_type())
        self.assertEqual(4, pc.get_num_parts())
        self.assertEqual(["store_id"], pc.get_fields_or_expr())
        entries = [
            PartitionDefinitionEntry(
                pdef_name="pNorth",
                pdef_type="p_values_in",
                pdef_value_list=["3", "5", "6", "9", "17"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pEast",
                pdef_type="p_values_in",
                pdef_value_list=["1", "2", "10", "11", "19", "20"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pWest",
                pdef_type="p_values_in",
                pdef_value_list=["4", "12", "13", "14", "18"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pCentral",
                pdef_type="p_values_in",
                pdef_value_list=["7", "8", "15", "16"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
        ]
        self.assertEqual(entries, pc.part_defs)

    def test_parts_list_cols(self):
        result = CreateParser.parse_partitions(PARTS_LIST_IN_WITH_COLUMNS)
        log.error(f"test_parts_list_cols4 Res: {result.dump()}")
        self.assertEqual("LIST", result.part_type)
        self.assertEqual("COLUMNS", result.p_subtype)
        self.assertEqual(4, len(result.part_defs))
        self.assertEqual(["city"], result.field_list.asList())
        pRegion_2 = result.part_defs[1]
        self.assertEqual("pRegion_2", pRegion_2.part_name)
        self.assertEqual(
            [["'Vimmerby'", "'Hultsfred'", "'Västervik'"]],
            pRegion_2.p_values_in.asList(),
        )

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_list_cols4 Model {pc}")
        self.assertEqual("LIST COLUMNS", pc.get_type())
        self.assertEqual(4, pc.get_num_parts())
        self.assertEqual(["city"], pc.get_fields_or_expr())
        entries = [
            PartitionDefinitionEntry(
                pdef_name="pRegion_1",
                pdef_type="p_values_in",
                pdef_value_list=["'Oskarshamn'", "'Högsby'", "'Mönsterås'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pRegion_2",
                pdef_type="p_values_in",
                pdef_value_list=["'Vimmerby'", "'Hultsfred'", "'Västervik'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pRegion_3",
                pdef_type="p_values_in",
                pdef_value_list=["'Nässjö'", "'Eksjö'", "'Vetlanda'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pRegion_4",
                pdef_type="p_values_in",
                pdef_value_list=["'Uppvidinge'", "'Alvesta'", "'Vaxjo'"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
        ]
        self.assertEqual(entries, pc.part_defs)

    def test_parts_key_empty(self):
        result = CreateParser.parse_partitions(PARTS_KEY_EMPTY)
        log.error(f"test_parts_key_empty5 Res: {result.dump()}")
        self.assertEqual("KEY", result.part_type)
        self.assertEqual("2", result.num_partitions)
        self.assertIsNone(result.get("part_defs", None))

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_key_empty5 Model {pc}")
        self.assertEqual("KEY", pc.get_type())
        self.assertEqual(2, pc.get_num_parts())
        self.assertEqual([], pc.part_defs)
        self.assertEqual([], pc.get_fields_or_expr())

    def test_parts_key_nonempty(self):
        result = CreateParser.parse_partitions(PARTS_KEY_NONEMPTY)
        log.error(f"test_parts_key_nonempty6 Res: {result.dump()}")
        self.assertEqual("KEY", result.part_type)
        self.assertEqual("2", result.num_partitions)
        self.assertEqual(["id1", "id2"], result.field_list.asList())
        self.assertIsNone(result.get("part_defs", None))

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_key_nonempty6 Model {pc}")
        self.assertEqual("KEY", pc.get_type())
        self.assertEqual(2, pc.get_num_parts())
        self.assertEqual([], pc.part_defs)
        self.assertEqual(["id1", "id2"], pc.get_fields_or_expr())

    def test_parts_hash(self):
        result = CreateParser.parse_partitions(PARTS_HASH)
        log.error(
            f"test_parts_hash7 Res: {result.dump()} Type: {type(result.p_hash_expr)}"
        )
        self.assertEqual("HASH", result.part_type)
        self.assertEqual("3", result.num_partitions)
        self.assertIsNone(result.get("part_defs", None))
        self.assertEqual("[['YEAR', ['hired']]]", f"{result.p_hash_expr}")

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_hash7 Model {pc}")
        self.assertEqual("HASH", pc.get_type())
        self.assertEqual(3, pc.get_num_parts())
        self.assertEqual([], pc.part_defs)
        self.assertEqual([["YEAR", ["hired"]]], pc.get_fields_or_expr())

    def test_parts_linear_hash(self):
        result = CreateParser.parse_partitions(PARTS_HASH_WITH_LINEAR)
        log.error(f"test_parts_linear_hash8 Res: {result.asDict()} ResList: {result}")
        self.assertEqual("HASH", result.part_type)
        self.assertEqual("LINEAR", result.p_subtype)
        self.assertEqual("4", result.num_partitions)
        self.assertIsNone(result.get("part_defs", None))
        self.assertEqual("[['YEAR', ['hired']]]", f"{result.p_hash_expr}")

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_linear_hash8 Model {pc}")
        self.assertEqual("LINEAR HASH", pc.get_type())
        self.assertEqual(4, pc.get_num_parts())
        self.assertEqual([], pc.part_defs)
        self.assertEqual([["YEAR", ["hired"]]], pc.get_fields_or_expr())

    def test_parts_key_nocount(self):
        result = CreateParser.parse_partitions(PARTS_KEY_NO_PARTCOUNT)
        log.error(f"test_parts_key_nocount9 Res: {result.dump()}")
        self.assertEqual("KEY", result.part_type)
        self.assertIsNone(result.get("num_partitions", None))

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_key_nocount9 Model {pc}")
        self.assertEqual("KEY", pc.get_type())
        self.assertEqual(1, pc.get_num_parts())
        self.assertEqual([], pc.part_defs)
        self.assertEqual([], pc.get_fields_or_expr())
        self.assertIsNone(pc.get_algo())  # No `ALGORITHM=\d` in input

    def test_parts_list_cols_intvals(self):
        result = CreateParser.parse_partitions(PARTS_LIST_IN_WITH_COLUMNS_INTVALS)
        log.error(f"test_parts_list_cols_intvals10 Res: {result.dump()}")
        self.assertEqual("LIST", result.part_type)
        self.assertEqual("COLUMNS", result.p_subtype)
        self.assertEqual(4, len(result.part_defs))
        self.assertEqual(["someid"], result.field_list.asList())
        pRegion_2 = result.part_defs[1]
        self.assertEqual("pRegion_2", pRegion_2.part_name)
        self.assertEqual(
            [["2", "6", "10", "14"]],
            pRegion_2.p_values_in.asList(),
        )

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_list_cols_intvals10 Model {pc}")
        self.assertEqual("LIST COLUMNS", pc.get_type())
        self.assertEqual(4, pc.get_num_parts())
        self.assertEqual(["someid"], pc.get_fields_or_expr())
        entries = [
            PartitionDefinitionEntry(
                pdef_name="pRegion_1",
                pdef_type="p_values_in",
                pdef_value_list=["1", "5", "9", "13"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pRegion_2",
                pdef_type="p_values_in",
                pdef_value_list=["2", "6", "10", "14"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pRegion_3",
                pdef_type="p_values_in",
                pdef_value_list=["3", "7", "11", "15"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="pRegion_4",
                pdef_type="p_values_in",
                pdef_value_list=["4", "8", "12", "16"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
        ]
        self.assertEqual(entries, pc.part_defs)

    def test_parts_key_algo(self):
        result = CreateParser.parse_partitions(PARTS_KEY_LINEAR_ALGO)
        log.error(f"test_parts_key_algo11 Res: {result.dump()}")
        self.assertEqual("KEY", result.part_type)
        self.assertEqual("LINEAR", result.p_subtype)
        self.assertEqual("10", result.num_partitions)
        self.assertEqual(["id"], result.field_list.asList())
        self.assertIsNone(result.get("part_defs", None))
        self.assertEqual(["2"], result.p_algo.asList())

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_key_algo11 Model {pc}")
        self.assertEqual("LINEAR KEY", pc.get_type())
        self.assertEqual(10, pc.get_num_parts())
        self.assertEqual([], pc.part_defs)
        self.assertEqual(["id"], pc.get_fields_or_expr())
        self.assertEqual(pc.get_algo(), 2)

    def test_parts_equality(self):
        # Comparison of PartitionConfig inititialized from diff sql (parts only)
        self.assertNotEqual(PARTS_RANGE_WITH_OPTS, PARTS_RANGE_WITH_OPTS_DUPE)

        res1 = CreateParser.parse_partitions(PARTS_RANGE_WITH_OPTS)
        log.error(f"test_parts_equality12 Orig Res: {res1.dump()}")

        res2 = CreateParser.parse_partitions(PARTS_RANGE_WITH_OPTS_DUPE)
        log.error(f"test_parts_equality12 DUPE Res: {res2.dump()}")

        pc1 = CreateParser.partition_to_model(res1)
        pc2 = CreateParser.partition_to_model(res2)
        self.assertEqual(pc1, pc2)

        pc3 = PartitionConfig()
        self.assertNotEqual(pc1, pc3)

    def test_parts_range_with_expr(self):
        result = CreateParser.parse_partitions(PARTS_RANGE_WITH_EXPR)
        log.error(f"test_parts_range_with_expr13 Res: {result.dump()}")

        self.assertEqual("RANGE", result.part_type)
        self.assertEqual(2, len(result.part_defs))
        self.assertEqual([["UNIX_TIMESTAMP", ["a"]]], result.p_expr.asList())

        # models.PartitionConfig from parsed result
        pc = CreateParser.partition_to_model(result)
        log.error(f"test_parts_range_with_expr13 Model {pc}")
        self.assertEqual("RANGE", pc.get_type())
        self.assertEqual(2, pc.get_num_parts())
        self.assertEqual([["UNIX_TIMESTAMP", ["a"]]], pc.get_fields_or_expr())
        entries = [
            PartitionDefinitionEntry(
                pdef_name="p1",
                pdef_type="p_values_less_than",
                pdef_value_list=["1554015600"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
            PartitionDefinitionEntry(
                pdef_name="p2",
                pdef_type="p_values_less_than",
                pdef_value_list=["1558249200"],
                pdef_comment=None,
                pdef_engine="INNODB",
            ),
        ]
        self.assertEqual(entries, pc.part_defs)

    def test_parts_range_many_engines(self):
        parts = (
            "PARTITION BY RANGE (store_id) ("
            "PARTITION p0 VALUES LESS THAN (6) ENGINE = 'innodb' COMMENT 'whatever',"
            "PARTITION p1 VALUES LESS THAN (11) ENGINE = 'rocksdb',"
            "PARTITION p2 VALUES LESS THAN (16),"
            "PARTITION p3 VALUES LESS THAN (21),"
            "PARTITION p4 VALUES LESS THAN maxvalue"
            ")"
        )
        result = CreateParser.parse_partitions(parts)
        log.error(f"test_parts_range_many_engines14 Res: {result.dump()}")
        # Varying engine types across part defs
        self.assertEqual(result.part_defs[0].pdef_engine, "INNODB")
        self.assertEqual(result.part_defs[1].pdef_engine, "ROCKSDB")

        with self.assertRaises(PartitionParseError):
            # There can be only on engine type
            _ = CreateParser.partition_to_model(result)
