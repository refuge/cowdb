%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
-module(cowdb_basic_tests).

-include("cowdb_tests.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(foreach(Fs), {foreach, fun setup/0, fun teardown/1, Fs}).

setup() ->
    {ok, Db} = cowdb:open(?tempfile()),
    Db.

teardown(Db) ->
    ok = cowdb:drop_db(Db).

open_close_test() ->
    {ok, Db} = cowdb:open(?tempfile()),
    ?assert(is_pid(Db)),
    ?assertMatch(ok, cowdb:drop_db(Db)).

database_info_test() ->
    FileName = ?tempfile(),
    {ok, Db} = cowdb:open(FileName),
    ?assertMatch({ok,  [{file_path, FileName},
                        {object_count,0},
                        {tx_count,1},
                        {tx_start,0},
                        {tx_end,0},
                        {compact_running,false},
                        {disk_size,4138},
                        {data_size,0},
                        {start_time, _},
                        {db_version,1}]}, cowdb:database_info(Db)),
    ok = cowdb:drop_db(Db).

basic_ops_test_() ->
    {
        "Test basic operations",
        ?foreach([
                fun should_create_kv/1,
                fun should_read_kv/1,
                fun should_create_kv_tuple/1,
                fun should_read_kv_tuple/1,
                fun should_update_kv/1,
                fun should_delete_kv/1,
                fun should_mget_kvs/1,
                fun should_mget_kvs_in_order/1,
                fun should_mget_not_found/1,
                fun should_add_multiple_kvs/1,
                fun should_put_multiple_kvs/1,
                fun should_delete_multiple_keys/1,
                fun should_add_remove/1,
                fun should_add_with_transact_function/1,
                fun should_query_with_transact_function/1,
                fun should_count/1,
                fun should_fold/1,
                fun should_fold_rev/1,
                fun should_stop_fold/1,
                fun should_fold_range/1,
                fun should_cancel_transact_fun/1,
                fun should_error_transact_fun/1,
                fun shoudl_catch_transact_fun_error/1
        ])
    }.

should_create_kv(Db) ->
    ?_assertMatch({ok, 1}, cowdb:put(Db, a, 1)).

should_read_kv(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    ?_assertMatch({ok, {a, 1}}, cowdb:get(Db, a)).

should_create_kv_tuple(Db) ->
    ?_assertMatch({ok, 1}, cowdb:put(Db, {a, 1})).

should_read_kv_tuple(Db) ->
    {ok, 1} = cowdb:put(Db, {a, 1}),
    ?_assertMatch({ok, {a, 1}}, cowdb:get(Db, a)).

should_update_kv(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:put(Db, a, 2),
    ?_assertMatch({ok, {a, 2}}, cowdb:get(Db, a)).

should_delete_kv(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    ?assertMatch({ok, {a, 1}}, cowdb:get(Db, a)),
    ?assertMatch({ok, 2}, cowdb:delete(Db, a)),
    ?_assertMatch(not_found,  cowdb:get(Db, a)).

should_mget_kvs(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:put(Db, b, 2),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}], cowdb:mget(Db, [a, b])).

should_mget_kvs_in_order(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:put(Db, b, 2),
    ?_assertMatch([{ok, {b, 2}}, {ok, {a, 1}}], cowdb:mget(Db, [b, a])).


should_mget_not_found(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:put(Db, b, 2),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, not_found],
                  cowdb:mget(Db, [a, b, c])).

should_add_multiple_kvs(Db) ->
    {ok, Tx} = cowdb:transact(Db,  [{add, a, 1},
                                    {add, b, 2},
                                    {add, c, 3}]),
    ?assertEqual(1, Tx),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, {ok, {c, 3}}],
                  cowdb:mget(Db, [a, b, c])).

should_put_multiple_kvs(Db) ->
    {ok, Tx} = cowdb:mput(Db,  [{a, 1}, {b, 2}, {c, 3}]),
    ?assertEqual(1, Tx),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, {ok, {c, 3}}],
                  cowdb:mget(Db, [a, b, c])).

should_delete_multiple_keys(Db) ->
    {ok, 1} = cowdb:mput(Db,  [{a, 1}, {b, 2}, {c, 3}]),
    ?assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, {ok, {c, 3}}],
                  cowdb:mget(Db, [a, b, c])),
    {ok, 2} = cowdb:mdelete(Db, [a, b, c]),
    ?_assertMatch([not_found, not_found, not_found],
                  cowdb:mget(Db, [a, b, c])).

should_add_remove(Db) ->
    {ok, Tx} = cowdb:transact(Db,  [{add, a, 1},
                                    {add, b, 2}]),
    ?assertEqual(1, Tx),
    ?assertMatch([{ok, {a, 1}}, {ok, {b, 2}}], cowdb:mget(Db, [a, b])),
    {ok, Tx2} = cowdb:transact(Db,  [{add, c, 3},
                                     {remove, b}]),
    ?assertEqual(2, Tx2),
    ?_assertMatch([{ok, {a, 1}}, not_found, {ok, {c, 3}}],
                  cowdb:mget(Db, [a, b, c])).

should_add_with_transact_function(Db) ->
    TransactFun = fun(_Db1) ->
            [{add, c, 3}]
    end,
    {ok, Tx} = cowdb:transact(Db,  [{add, a, 1},
                                    {add, b, 2},
                                    {fn, TransactFun}]),
    ?assertEqual(1, Tx),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, {ok, {c, 3}}],
                  cowdb:mget(Db, [a, b, c])).

should_query_with_transact_function(Db) ->
    TransactFun = fun(Db1) ->
            {ok, {a, Val}} = cowdb:get(Db1, a),
            [{add, d, Val}]
    end,
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:transact(Db, [{fn, TransactFun}]),
    ?_assertMatch({ok, {d, 1}}, cowdb:get(Db, d)).

should_count(Db)->
    {ok, 1} = cowdb:transact(Db, [{add, a, 1},
                                  {add, b, 2},
                                  {add, c, 3},
                                  {add, d, 4},
                                  {add, e, 5}]),

    ?_assertEqual({ok, 5}, cowdb:count(Db)).

should_fold(Db) ->
    FoldFun = fun(KV, Acc) ->
            {ok, [KV |Acc]}
    end,

    {ok, 1} = cowdb:transact(Db, [{add, a, 1},
                                  {add, b, 2},
                                  {add, c, 3},
                                  {add, d, 4},
                                  {add, e, 5}]),
    ?_assertMatch({ok, [{e, 5}, {d, 4}, {c, 3}, {b, 2}, {a, 1}]},
                  cowdb:fold(Db, FoldFun, [])).

should_fold_rev(Db) ->
    FoldFun = fun(KV, Acc) ->
            {ok, [KV |Acc]}
    end,

    {ok, 1} = cowdb:transact(Db, [{add, a, 1},
                                  {add, b, 2},
                                  {add, c, 3},
                                  {add, d, 4},
                                  {add, e, 5}]),

    ?_assertMatch({ok, [{a, 1}, {b, 2}, {c, 3}, {d, 4}, {e, 5}]},
                  cowdb:fold(Db, FoldFun, [], [{dir, rev}])).

should_stop_fold(Db) ->
    FoldFun = fun
        ({d, _}, Acc) ->
            {stop, Acc};
        (KV, Acc) ->
            {ok, [KV |Acc]}
    end,

    {ok, 1} = cowdb:transact(Db, [{add, a, 1},
                                  {add, b, 2},
                                  {add, c, 3},
                                  {add, d, 4},
                                  {add, e, 5}]),
    ?_assertMatch({ok, [{c, 3}, {b, 2}, {a, 1}]},
                  cowdb:fold(Db, FoldFun, [])).

should_fold_range(Db) ->
    FoldFun = fun(KV, Acc) ->
            {ok, [KV |Acc]}
    end,

    {ok, 1} = cowdb:transact(Db, [{add, a, 1},
                                  {add, b, 2},
                                  {add, c, 3},
                                  {add, d, 4},
                                  {add, e, 5}]),
    ?_assertMatch({ok, [{d, 4}, {c, 3}]},
                  cowdb:fold(Db, FoldFun, [], [{start_key, c},
                                               {end_key, d}])).


should_cancel_transact_fun(Db) ->
    TransactFun = fun(_Db1) ->
           cancel
    end,
    Reply = cowdb:transact(Db,  [{fn, TransactFun}]),
    ?_assertMatch({error, canceled}, Reply).

should_error_transact_fun(Db) ->
    TransactFun = fun(_Db1) ->
        {error, conflict}
    end,
    Reply = cowdb:transact(Db,  [{fn, TransactFun}]),
    ?_assertMatch({error, conflict}, Reply).

shoudl_catch_transact_fun_error(Db) ->
    TransactFun = fun(_Db1) ->
            throw(badarg)
    end,
    Reply = cowdb:transact(Db,  [{fn, TransactFun}]),
    ?_assertMatch(badarg, Reply).
