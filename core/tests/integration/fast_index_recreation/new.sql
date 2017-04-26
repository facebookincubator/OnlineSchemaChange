-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree. An additional grant
-- of patent rights can be found in the PATENTS file in the same directory.

CREATE TABLE IF NOT EXISTS `table1` (
  `id` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `id2` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `id3` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `id4` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `data` mediumtext COLLATE latin1_bin NOT NULL,
  PRIMARY KEY (`id`) COMMENT 'id',
  KEY `key1` (`id2`),
  KEY `key2` (`id3`),
  KEY `key3` (`id4`)
) ENGINE=InnoDB;
