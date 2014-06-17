% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.
%
-module(cowdb_compaction_tests).

-include("cowdb_tests.hrl").

basic_compaction_test() ->
    {ok, Db} = cowdb:open(?tempfile()),
    {ok, 1} = cowdb:transact(Db, [{add, a, 1},
                                  {add, b, 2}]),
    {ok, 2} = cowdb:transact(Db, [{add, c, 3},
                                  {add, d, 4},
                                  {add, e, 5}]),

    {ok, DbInfo0} = cowdb:db_info(Db),
    DiskSize0 = proplists:get_value(disk_size, DbInfo0),
    DataSize0 = proplists:get_value(data_size, DbInfo0),
    ok = cowdb:compact(Db),
    timer:sleep(1000),
    {ok, DbInfo2} = cowdb:db_info(Db),
    DiskSize2 = proplists:get_value(disk_size, DbInfo2),
    DataSize2 = proplists:get_value(data_size, DbInfo2),
    ?assert(DiskSize2 < DiskSize0),
    ?assertEqual(DataSize2, DataSize0),
    ok = cowdb:drop_db(Db).

