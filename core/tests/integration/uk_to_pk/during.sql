-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree.

delete from table1 where id=1;
update table1 set data = 'b' where id=2;
update table1 set id=4, data = 'b' where id=3;
insert into table1 values (5, 'c');
