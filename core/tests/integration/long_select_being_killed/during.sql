-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree.

begin;
select id from table1 where id = 1;
select id, sleep(200) from table1 where id = 1;
commit;
