-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree.

CREATE TABLE `table1` (
  `id` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `data` mediumtext CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  PRIMARY KEY (`id`) COMMENT 'id'
) ENGINE=InnoDB charset=latin1
/*!50100 PARTITION BY RANGE (id)
(PARTITION p1 VALUES LESS THAN (2) ENGINE = InnoDB,
 PARTITION p2 VALUES LESS THAN (3) ENGINE = InnoDB,
 PARTITION p3 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
