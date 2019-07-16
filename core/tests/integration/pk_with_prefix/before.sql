-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree.

drop TABLE IF EXISTS `table1` ;
CREATE TABLE IF NOT EXISTS `table1` (
  `id` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `name` varchar(4000) NOT NULL DEFAULT '0' ,
  `data` mediumtext COLLATE latin1_bin NOT NULL,
  PRIMARY KEY (`id`, `name`(10)) COMMENT 'id'
) ENGINE=InnoDB;
insert into table1 values (1, 'aaa', 'a');
insert into table1 values (2, 'bbb', 'a');
