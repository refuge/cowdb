%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
-module(cowdb_btree_copy_tests).

-include_lib("include/cowdb.hrl").
-include("cowdb_tests.hrl").


setup_copy(_) ->
    ReduceFun = fun(reduce, KVs) ->
            length(KVs);
        (rereduce, Reds) ->
            lists:sum(Reds)
    end,

    OriginalFileName = ?tempfile(),
    CopyFileName = OriginalFileName ++ ".copy",
    {ok, Fd} = cowdb_file:open(OriginalFileName, [create, overwrite]),
    {ok, FdCopy} = cowdb_file:open(CopyFileName, [create, overwrite]),
    {ReduceFun, OriginalFileName, CopyFileName, Fd, FdCopy}.

teardown(_, {_, OriginalFileName, CopyFileName, Fd, FdCopy}) ->
    ok = cowdb_file:close(Fd),
    ok = cowdb_file:close(FdCopy),
    ok = file:delete(OriginalFileName),
    ok = file:delete(CopyFileName).

btree_copy_test_() ->
    TNumItems = [50, 100, 300, 700, 811, 2333, 6594, 9999, 15003, 21477,
                 38888, 66069, 150123, 420789, 711321],
    {
        "Copy BTree",
        {
            foreachx,
            fun setup_copy/1, fun teardown/2,
            [{N, fun  should_copy_btree/2} || N <- TNumItems]
        }
    }.

btree_copy_compressed_test_() ->
    TNumItems = [50, 100, 300, 700, 811, 2333, 6594, 9999, 15003, 21477,
                 38888, 66069, 150123, 420789, 711321],
    {
        "Copy Compressed BTree",
        {
            foreachx,
            fun setup_copy/1, fun teardown/2,
            [{N, fun  should_copy_compressed_btree/2} || N <- TNumItems]
        }
    }.

should_copy_btree(NumItems, {ReduceFun, _OriginalFileName, _CopyFileName,
                             Fd, FdCopy}) ->
    KVs = [{I, I} || I <- lists:seq(1, NumItems)],
    {ok, Btree} = make_btree(Fd, KVs, ReduceFun),

    {_, Red, _} = cowdb_btree:get_state(Btree),

    CopyCallback = fun(KV, Acc) -> {KV, Acc + 1} end,
    {ok, RootCopy, FinalAcc} = cowdb_btree_copy:copy(
        Btree, FdCopy, [{before_kv_write, {CopyCallback, 0}}]),

    ?assertMatch(FinalAcc, length(KVs)),

    {ok, BtreeCopy} = cowdb_btree:open(
        RootCopy, FdCopy, [{compression, none}, {reduce, ReduceFun}]),

    %% check copy
    {_, RedCopy, _} = cowdb_btree:get_state(BtreeCopy),
    ?assertMatch(Red, RedCopy),
    {ok, _, CopyKVs} = cowdb_btree:fold(
        BtreeCopy,
        fun(KV, _, Acc) -> {ok, [KV | Acc]} end,
        [], []),
    ?_assertMatch(KVs, lists:reverse(CopyKVs)).

should_copy_compressed_btree(NumItems, {ReduceFun, _OriginalFileName,
                                        _CopyFileName, Fd, FdCopy}) ->

    KVs = [{I, I} || I <- lists:seq(1, NumItems)],
    {ok, Btree} = make_btree(Fd, KVs, ReduceFun, snappy),

    {_, Red, _} = cowdb_btree:get_state(Btree),

    CopyCallback = fun(KV, Acc) -> {KV, Acc + 1} end,
    {ok, RootCopy, FinalAcc} = cowdb_btree_copy:copy(
        Btree, FdCopy, [{before_kv_write, {CopyCallback, 0}}]),

    ?assertMatch(FinalAcc, length(KVs)),

    {ok, BtreeCopy} = cowdb_btree:open(
        RootCopy, FdCopy, [{compression, snappy}, {reduce, ReduceFun}]),

    %% check copy
    {_, RedCopy, _} = cowdb_btree:get_state(BtreeCopy),
    ?assertMatch(Red, RedCopy),
    {ok, _, CopyKVs} = cowdb_btree:fold(
        BtreeCopy,
        fun(KV, _, Acc) -> {ok, [KV | Acc]} end,
        [], []),
    ?_assertMatch(KVs, lists:reverse(CopyKVs)).




make_btree(Fd, KVs, ReduceFun) ->
    make_btree(Fd, KVs, ReduceFun, none).

make_btree(Fd, KVs, ReduceFun, Compression) ->

    {ok, Btree} = cowdb_btree:open(nil, Fd, [{compression, Compression},
                                           {reduce, ReduceFun}]),
    {ok, Btree2} = cowdb_btree:add_remove(Btree, KVs, []),
    {_, Red, _} = cowdb_btree:get_state(Btree2),
    ?assertMatch(Red, length(KVs)),
    ok = cowdb_file:sync(Fd),
    {ok, Btree2}.
