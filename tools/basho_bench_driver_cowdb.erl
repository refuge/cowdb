-module(basho_bench_driver_cowdb).

-export([new/1,
         run/4]).

-include("cowdb.hrl").
-include_lib("basho_bench/include/basho_bench.hrl").


new(_Id) ->
    %% Make sure bitcask is available
    case code:which(cowdb) of
        non_existing ->
            ?FAIL_MSG("~s requires cowdb to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,
    Config = basho_bench_config:get(cowdb_flags, []),
    case cowdb:open("test.cowdb", Config) of
        {ok, Db} ->
            {ok, Db};
        {error, Reason} ->
            ?FAIL_MSG("Failed to open the database: ~p\n", [Reason])
    end.

run(get, KeyGen, _ValueGen, Db) ->
    case cowdb:get(Db, KeyGen()) of
        {ok, _} ->
            {ok, Db};
        not_found ->
            {ok, Db};
        Error ->
            Error
    end;
run(put, KeyGen, ValueGen, Db) ->
    case cowdb:put(Db, KeyGen(), ValueGen()) of
        {ok, _} ->
            {ok, Db};
        Error ->
            Error
    end;
run(delete, KeyGen, _ValueGen, Db) ->
    case cowdb:delete(Db, KeyGen()) of
        {ok, _} ->
            {ok, Db};
        Error ->
            Error
    end;

run(fold_100, _KeyGen, _ValueGen, Db) ->
    case cowdb:fold(Db, fun(_KV, Count1) ->
                    Count2 = Count1 + 1,
                    if Count2 =:= 100 ->
                            {stop, Count2};
                        true ->
                            {ok, Count2}
                    end
            end, 0) of
        {ok, Count} when Count >= 0; Count =< 100 ->
            {ok, Db};
        {ok, Count} ->
            {error, {bad_fold_count, Count}};
        Error ->
            Error
    end.
