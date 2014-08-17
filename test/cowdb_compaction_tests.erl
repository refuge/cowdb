%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
-module(cowdb_compaction_tests).

-include("cowdb_tests.hrl").

basic_compaction_test() ->
    {ok, Db} = cowdb:open(?tempfile()),
    {ok, 1} = cowdb:transact(Db, [{add, a, 1},
                                  {add, b, 2}]),
    {ok, 2} = cowdb:transact(Db, [{add, c, 3},
                                  {add, d, 4},
                                  {add, e, 5}]),

    {ok, DbInfo0} = cowdb:database_info(Db),
    DiskSize0 = proplists:get_value(disk_size, DbInfo0),
    DataSize0 = proplists:get_value(data_size, DbInfo0),
    ok = cowdb:compact(Db),
    timer:sleep(1000),
    {ok, DbInfo2} = cowdb:database_info(Db),
    DiskSize2 = proplists:get_value(disk_size, DbInfo2),
    DataSize2 = proplists:get_value(data_size, DbInfo2),
    ?assert(DiskSize2 < DiskSize0),
    ?assertEqual(DataSize2, DataSize0),
    ok = cowdb:drop_db(Db).

