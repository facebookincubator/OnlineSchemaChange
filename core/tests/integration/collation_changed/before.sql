-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree.

drop TABLE IF EXISTS `table1` ;
CREATE TABLE IF NOT EXISTS `table1` (
  `id` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `data` varchar(20) NOT NULL,
  PRIMARY KEY (`data`) COMMENT 'id'
) ENGINE=InnoDB;
insert into table1 values (1,'a11');
insert into table1 values (2,'b22');
insert into table1 values (3,'c33');
insert into table1 values (4,'A44');
insert into table1 values (5,'B55');
insert into table1 values (6,'C66');
