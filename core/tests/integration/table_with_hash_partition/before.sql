-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree.

DROP TABLE IF EXISTS `table1` ;
CREATE TABLE `table1` (
  `id` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `data` mediumtext COLLATE latin1_bin NOT NULL,
  PRIMARY KEY (`id`) COMMENT 'id'
) ENGINE=InnoDB DEFAULT CHARSET=latin1
/*!50100 PARTITION BY HASH (id)
PARTITIONS 2 */;
INSERT INTO table1 VALUES (1,'a');
INSERT INTO table1 VALUES (2,'b');
