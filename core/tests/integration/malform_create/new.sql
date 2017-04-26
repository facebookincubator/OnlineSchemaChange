-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree. An additional grant
-- of patent rights can be found in the PATENTS file in the same directory.

CREATE WRONG SYNTAX TABLE IF NOT EXISTS `table1` (
  `id1` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `id2` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `id3` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `id4` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  PRIMARY KEY (`id2`, `id3`) COMMENT 'id'
) ENGINE=InnoDB;
