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
-module(cowdb_log_tests).

-include("cowdb_tests.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(foreach(Fs), {foreach, fun setup/0, fun teardown/1, Fs}).

setup() ->
    {ok, Db} = cowdb:open(?tempfile()),
    Db.

teardown(Db) ->
    ok = cowdb:drop_db(Db).

log_test_() ->
    {
        "Test snapshotting and log features",
        ?foreach([
                fun should_log_transactions/1,
                fun should_fetch_log_in_range/1,
                fun log_is_reset_after_compaction/1,
                fun should_keep_last_transaction_id_after_compaction/1
        ])
    }.


snapshot_test() ->
    {ok, Db} = cowdb:open(?tempfile()),
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:transact(Db, [{add, b, 2},
                                  {add, c, 3}]),
    {ok, 3} = cowdb:transact(Db, [{remove, b},
                                  {add, d, 4}]),

    {ok, Db1} = cowdb:get_snapshot(Db, 1),
    ?assertMatch([{ok, {a, 1}}, not_found, not_found, not_found],
                 cowdb:mget(Db1, [a, b, c, d])),

    {ok, Db2} = cowdb:get_snapshot(Db, 2),
    ?assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, {ok, {c, 3}}, not_found],
                 cowdb:mget(Db2, [a, b, c, d])),

    {ok, Db3} = cowdb:get_snapshot(Db, 3),
    ?assertMatch([{ok, {a, 1}}, not_found, {ok, {c, 3}}, {ok, {d, 4}}],
                 cowdb:mget(Db3, [a, b, c, d])),

    ok = cowdb:drop_db(Db).


should_log_transactions(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:transact(Db, [{add, b, 2},
                                  {add, c, 3}]),
    {ok, 3} = cowdb:transact(Db, [{remove, b},
                                  {add, d, 4}]),

    LogFun = fun(Got, Acc) ->
            {ok, [Got |Acc]}
    end,
    ?_assertMatch({ok, 4, [{3, add, {d, 4}, _},
                           {3, remove, {b, 2}, _},
                           {2, add, {c, 3}, _},
                           {2, add, {b, 2}, _},
                           {1, add, {a, 1}, _}]},
                   cowdb:log(Db, 0, 3, LogFun, [])).

should_fetch_log_in_range(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:transact(Db, [{add, b, 2},
                                  {add, c, 3}]),
    {ok, 3} = cowdb:transact(Db, [{remove, b},
                                  {add, d, 4}]),

    LogFun = fun(Got, Acc) ->
            {ok, [Got |Acc]}
    end,
    ?_assertMatch({ok, 3, [{2, add, {c, 3}, _},
                           {2, add, {b, 2}, _},
                           {1, add, {a, 1}, _}]},
                   cowdb:log(Db, 1, 2, LogFun, [])).

log_is_reset_after_compaction(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:transact(Db, [{add, b, 2},
                                  {add, c, 3}]),
    {ok, 3} = cowdb:transact(Db, [{remove, b},
                                  {add, d, 4}]),

    {ok, DbInfo0} = cowdb:database_info(Db),
    TxCount0 = proplists:get_value(tx_count, DbInfo0),
    ok = cowdb:compact(Db),
    timer:sleep(1000),
    {ok, DbInfo2} = cowdb:database_info(Db),
    TxCount2 = proplists:get_value(tx_count, DbInfo2),
    ?_assertEqual({4, 1}, {TxCount0, TxCount2}).

should_keep_last_transaction_id_after_compaction(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:transact(Db, [{add, b, 2},
                                  {add, c, 3}]),
    {ok, 3} = cowdb:transact(Db, [{remove, b},
                                  {add, d, 4}]),

    {ok, DbInfo0} = cowdb:database_info(Db),
    TxEnd0 =  proplists:get_value(tx_end, DbInfo0),
    ok = cowdb:compact(Db),
    timer:sleep(1000),
    {ok, DbInfo2} = cowdb:database_info(Db),
    TxStart2 = proplists:get_value(tx_start, DbInfo2),
    ?_assertEqual(TxEnd0, TxStart2).
