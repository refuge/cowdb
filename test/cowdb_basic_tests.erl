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
-module(cowdb_basic_tests).

-include("cowdb_tests.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(foreach(Fs), {foreach, fun setup/0, fun teardown/1, Fs}).

setup() ->
    {ok, Db} = cowdb:open(?tempfile()),
    Db.

teardown(Db) ->
    ok = cowdb:delete_db(Db).

open_close_test() ->
    {ok, Db} = cowdb:open(?tempfile()),
    ?assert(is_pid(Db)),
    ?assertMatch(ok, cowdb:delete_db(Db)).

basic_ops_test_() ->
    {
        "Test basic operations",
        ?foreach([
                fun should_create_kv/1,
                fun should_read_kv/1,
                fun should_delete_kv/1,
                fun should_lookup_kvs/1,
                fun should_lookup_kvs_in_order/1,
                fun should_lookup_not_found/1,
                fun should_add_multiple_kvs/1,
                fun should_add_remove/1,
                fun should_add_with_transact_function/1,
                fun should_query_with_transact_function/1
        ])
    }.

should_create_kv(Db) ->
    ?_assertMatch({ok, 1}, cowdb:put(Db, a, 1)).

should_read_kv(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    ?_assertMatch({ok, {a, 1}}, cowdb:get(Db, a)).

should_delete_kv(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    ?assertMatch({ok, {a, 1}}, cowdb:get(Db, a)),
    ?assertMatch({ok, 2}, cowdb:delete(Db, a)),
    ?_assertMatch(not_found,  cowdb:get(Db, a)).

should_lookup_kvs(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:put(Db, b, 2),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}], cowdb:lookup(Db, [a, b])).

should_lookup_kvs_in_order(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:put(Db, b, 2),
    ?_assertMatch([{ok, {b, 2}}, {ok, {a, 1}}], cowdb:lookup(Db, [b, a])).


should_lookup_not_found(Db) ->
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:put(Db, b, 2),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, not_found],
                  cowdb:lookup(Db, [a, b, c])).

should_add_multiple_kvs(Db) ->
    {ok, Tx} = cowdb:transact(Db,  [{add, a, 1},
                                    {add, b, 2},
                                    {add, c, 3}]),
    ?assertEqual(1, Tx),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, {ok, {c, 3}}],
                  cowdb:lookup(Db, [a, b, c])).


should_add_remove(Db) ->
    {ok, Tx} = cowdb:transact(Db,  [{add, a, 1},
                                    {add, b, 2}]),
    ?assertEqual(1, Tx),
    ?assertMatch([{ok, {a, 1}}, {ok, {b, 2}}], cowdb:lookup(Db, [a, b])),
    {ok, Tx2} = cowdb:transact(Db,  [{add, c, 3},
                                     {remove, b}]),
    ?assertEqual(2, Tx2),
    ?_assertMatch([{ok, {a, 1}}, not_found, {ok, {c, 3}}],
                  cowdb:lookup(Db, [a, b, c])).

should_add_with_transact_function(Db) ->
    TransactFun = fun(_Db1) ->
            [{add, c, 3}]
    end,
    {ok, Tx} = cowdb:transact(Db,  [{add, a, 1},
                                    {add, b, 2},
                                    {fn, TransactFun}]),
    ?assertEqual(1, Tx),
    ?_assertMatch([{ok, {a, 1}}, {ok, {b, 2}}, {ok, {c, 3}}],
                  cowdb:lookup(Db, [a, b, c])).

should_query_with_transact_function(Db) ->
    TransactFun = fun(Db1) ->
            {ok, {a, Val}} = cowdb:get(Db1, a),
            [{add, d, Val}]
    end,
    {ok, 1} = cowdb:put(Db, a, 1),
    {ok, 2} = cowdb:transact(Db, [{fn, TransactFun}]),
    ?_assertMatch({ok, {d, 1}}, cowdb:get(Db, d)).
