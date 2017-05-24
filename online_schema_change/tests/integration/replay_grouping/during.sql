-- Copyright (c) 2017-present, Facebook, Inc.
-- All rights reserved.
-- 
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree. An additional grant
-- of patent rights can be found in the PATENTS file in the same directory.

delete from table1 where id=1;
delete from table1 where id=2;
update table1 set data='b' where id=3;
insert into table1 values (4, 'c');
insert into table1 values (5, 'c');
insert into table1 values (6, 'c');
