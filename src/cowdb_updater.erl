%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
%% @private
-module(cowdb_updater).

%% PUBLIC API
-export([start_link/3]).
-export([close/1]).
-export([transact/3]).
-export([compact/2, cancel_compact/1]).
-export([get_db/1]).
-export([req/2]).


%% proc lib export
-export([init/3]).

%% compaction util export
-export([init_db/6]).

-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").

-type trans_type() :: version_change | update.
-export_type([trans_type/0]).

start_link(FilePath, Options, DbPid) ->
    proc_lib:start_link(?MODULE, init, [FilePath, Options, DbPid]).


%% @doc get latest db state.
-spec get_db(pid()) -> cowdb:db().
get_db(Pid) ->
    req(Pid, get_db).

%% @doc send a transaction and wait for its result.
transact(UpdaterPid, Ops, Timeout) ->
    req(UpdaterPid, {transact, Ops, Timeout}).

compact(UpdaterPid, Options) ->
    req(UpdaterPid, {start_compact, Options}).

cancel_compact(UpdaterPid) ->
    req(UpdaterPid, cancel_compact).


close(UpdaterPid) ->
    req(UpdaterPid, close).


init(FilePath, Options, DbPid) ->
    case catch do_open_db(FilePath, Options, DbPid) of
        {ok, Db} ->
            proc_lib:init_ack({ok, self()}),
            updater_loop(Db);
        Error ->
            error_logger:info_msg("error initialising the database ~p: ~p",
                                   [FilePath, Error]),
            proc_lib:init_ack(Error)
    end.

updater_loop(Db) ->
    receive
        ?COWDB_CALL(From, Req) ->
            do_apply_op(Req, From, Db);
        {'EXIT', Pid, Reason} when Pid =:= Db#db.db_pid ->
            ok = do_stop(Db),
            exit(Reason);
        {'EXIT', Pid, Reason} when Pid =:= Db#db.fd ->
            ok = do_stop(Db),
            exit(Reason);
        {'EXIT', Pid, Reason} when Pid =:= Db#db.reader_fd ->
            ok = do_stop(Db),
            exit(Reason);
        {'EXIT', _Pid, normal}  ->
            %% a compaction happened, old writer and reader are exiting.
            updater_loop(Db);
        Msg ->
            error_logger:format("** cowdb_updater: unexpected message"
                                "(ignored): ~w~n", [Msg]),
            updater_loop(Db)
    end.

do_apply_op(Req, From, Db) ->
    try apply_op(Req, From, Db) of
        ok ->
            updater_loop(Db);
        Db2 when is_record(Db2, db) ->
            updater_loop(Db2);
        {more, Req1, From1, Db1} ->
            do_apply_op(Req1, From1, Db1)
    catch
        exit:normal ->
            exit(normal);
        _:Error ->
            FilePath = Db#db.file_path,
            error_logger:format
                      ("** cowdb: Bug was found when accessing database ~w~n",
                       [FilePath]),
            if
                From =/= self() ->
                    From ! {self(), {error, {cowdb_bug, FilePath, Req, Error}}},
                    ok;
                true -> % auto_save | may_grow | {delayed_write, _}
                    ok
            end,
            updater_loop(Db)
    end.



apply_op(Req, From, Db) ->
    #db{tid=LastTid, db_pid=DbPid, compactor_info=CompactInfo}=Db,

    case Req of
        {transact, OPs, Options} ->
            TransactId = LastTid + 1,
            {Reply, Db2} = case catch handle_transaction(TransactId, OPs,
                                                          Options, Db) of
                {ok, Db1} ->
                    gen_server:cast(DbPid, {db_updated, Db1}),
                    {{ok, TransactId}, Db1};
                Error ->
                    {Error, Db}
            end,

            %% send the reply to the client
            From ! {self(), Reply},

            %% check if we need to compact, if true it will trigger a
            %% background_compact message.
            case maybe_compact(Db2) of
                true ->
                    {more, background_compact, self(), Db2};
                false ->
                    Db2
            end;
        background_compact ->
            case Db#db.compactor_info of
                nil ->
                    Pid = spawn_link(fun() ->
                                    cowdb_compaction:start(Db, [])
                            end),
                    Db1 = Db#db{compactor_info=Pid},
                    ok = gen_server:cast(DbPid, {db_updated, Db1}),
                    Db1;
                _ ->
                    ok
            end;
        {start_compact, Options} ->
            case Db#db.compactor_info of
                nil ->
                    Pid = spawn_link(fun() ->
                                    cowdb_compaction:start(Db, Options)
                            end),
                    Db2 = Db#db{compactor_info=Pid},
                    ok = gen_server:cast(DbPid, {db_updated, Db2}),
                    From ! {self(), ok},
                    Db2;
                _CompactorPid ->
                    From ! {self, ok},
                    ok
            end;
        {compact_done, _CompactFile}
                when CompactInfo =:= nil ->
            %% the compactor already exited
            ok;
        {compact_done, CompactFile} ->
            #db{fd=Fd, reader_fd=ReaderFd, file_path=FilePath}=Db,
            {ok, NewFd} = cbt_file:open(CompactFile),
            {ok, NewReaderFd} = cbt_file:open(CompactFile, [read_only]),
            {ok, NewHeader, _Pos} = cbt_file:read_header(NewFd),
            #db{tid=Tid} = NewDb = cowdb_util:init_db(NewHeader, Db#db.db_pid,
                                                      NewFd, NewReaderFd,
                                                      FilePath,
                                                      Db#db.options),

            %% check if we need to relaunch the compaction or not.
            case Db#db.tid == Tid of
                true ->
                    %% send to the db the new value
                    ok = gen_server:cast(DbPid, {db_updated, NewDb}),

                    %% we are now ready to switch the file
                    %% prevent write to the old file
                    cbt_file:close(Fd),

                    %% rename the old file path
                    OldFilePath = FilePath ++ ".old",
                    cbt_file:rename(ReaderFd, OldFilePath),

                    %% rename the compact file
                    cbt_file:rename(NewFd, FilePath),
                    gen_server:call(NewReaderFd, {set_path, FilePath},
                                    infinity),
                    cbt_file:sync(NewFd),
                    cbt_file:sync(NewReaderFd),

                    %% now delete the old file
                    cowdb_util:delete_file(OldFilePath),

                    %% tell the compactor it can close.
                    From ! {self(), ok},
                    NewDb;
                false ->
                    cbt_file:close(NewFd),
                    cbt_file:close(NewReaderFd),
                    From ! {self(), {retry, Db}},
                    Db
            end;
        cancel_compact ->
            Db2 = case Db#db.compactor_info of
                nil ->
                    Db;
                CompactorPid ->
                    cowdb_util:shutdown_sync(CompactorPid),
                    cowdb_compaction:delete_compact_file(Db),
                    Db#db{compactor_info=nil}
            end,
            From ! {self(), ok},
            Db2;
        get_db ->
            From ! {self(), {ok, Db}},
            ok;
        close ->
            do_stop(Db),
            From ! {self(), ok},
            exit(normal)

    end.

do_stop(#db{fd=Fd, reader_fd=ReaderFd}) ->
    ok = cbt_file:close(Fd),
    ok = cbt_file:close(ReaderFd),
    ok.


do_open_db(FilePath, Options, DbPid) ->
    %% set open options
    OpenOptions = case proplists:get_value(override, Options, false) of
        true -> [create_if_missing, override];
        false -> [create_if_missing]
    end,


    %% open the writer fd
    case cbt_file:open(FilePath, OpenOptions) of
        {ok, Fd} ->
            %% open the the reader file
            {ok, ReaderFd} = cbt_file:open(FilePath, [read_only]),
            process_flag(trap_exit, true),

            %% open the header or initialize it.
            Header = case cbt_file:read_header(Fd) of
                {ok, Header1, _Pos} ->
                    Header1;
                no_valid_header ->
                    Header1 = #db_header{},
                    {ok, _} = cbt_file:write_header(Fd, Header1),
                    Header1
            end,
            %% initialize the database.
            init_db(Header, DbPid, Fd, ReaderFd, FilePath, Options);
        Error ->
            Error
    end.

init_db(Header, DbPid, Fd, ReaderFd, FilePath, Options) ->
    Db = cowdb_util:init_db(Header, DbPid, Fd, ReaderFd, FilePath,
                             Options),

    #db{tid=Tid, log=LogBt} = Db,

    ShouldInit = Tid < 0,
    InitFunc = proplists:get_value(init_func, Options),

    case {ShouldInit, InitFunc} of
        {true, undefined} ->
            Transaction = {0, #transaction{by_id=nil,
                                           ops=[],
                                           ts= cowdb_util:timestamp()}},
            {ok, LogBt2} = cbt_btree:add(LogBt, [Transaction]),
            cowdb_util:commit_transaction(0, Db#db{log=LogBt2});
        {false, undefined} ->
            {ok, Db};
        {_, InitFunc} ->
            TransactId = Tid + 1,
            %% initialize the database with the init function.
            do_transaction(fun() ->
                        case call_init(InitFunc, Tid, TransactId, Db) of
                            {ok, Ops} ->
                                %% an init function can return an initial
                                %% transaction.
                                Ts = cowdb_util:timestamp(),
                                run_transaction(Ops, {[], []}, [], TransactId,
                                                Ts, Db);
                            Error ->
                                Error
                        end
                end, TransactId)
    end.

handle_transaction(TransactId, OPs, Timeout, Db) ->
    UpdaterPid = self(),

    %% spawn a transaction so we can cancel it if needed.
    TransactPid = spawn(fun() ->
                    %% execute the transaction
                    Resp = do_transaction(fun() ->
                                    Ts = cowdb_util:timestamp(),
                                    run_transaction(OPs, {[], []}, [],
                                                    TransactId, Ts, Db)
                            end, TransactId),

                    UpdaterPid ! {TransactId, {done, Resp}}
            end),

    %% wait for the result
    receive
        {TransactId, {done, cancel}} ->
            rollback_transaction(Db),
            {error, canceled};
        {TransactId, {done, Resp}} ->
            Resp;
        {TransactId, cancel} ->
            cowdb_util:shutdown_sync(TransactPid),
            rollback_transaction(Db),
            {error, canceled}
    after Timeout ->
            rollback_transaction(Db),
            {error, timeout}
    end.

%% rollback to the latest root
%% we don't try to edit in-place, instead we take
%% the latest known header and append it to the database file
rollback_transaction(#db{fd=Fd, header=Header}) ->
    ok= cbt_file:sync(Fd),
    {ok, _Pos} = cbt_file:write_header(Fd, Header),
    ok= cbt_file:sync(Fd),
    ok.

run_transaction([], {ToAdd, ToRem}, Log0, TransactId, Ts,
                #db{by_id=IdBt, log=LogBt}=Db) ->
    %% we atomically store the transaction.
    {ok, Found, IdBt2} = cbt_btree:query_modify(IdBt, ToRem, ToAdd, ToRem),

    RemValues = [{RemKey, {RemKey, RemPointer, TransactId, Ts}}
                 || {ok, {_, {RemKey, RemPointer, _, _}}} <- Found],

    %% reconstruct transaction operations for the log.
    %% currently, this is very inefficient and, probably,  there is better
    %% way to do it.
    Log = lists:foldl(fun
                ({add, _}=Entry, Acc) ->
                    [Entry | Acc];
                ({remove, RemKey}, Acc) ->
                    case lists:keyfind(RemKey, 1, RemValues) of
                        {_K, Val} ->
                            [{remove, Val} |Acc];
                        _ ->
                            Acc
                    end
            end, [], Log0),

    %% store the new log
    Transaction = {TransactId, #transaction{tid=TransactId,
                                            by_id=cbt_btree:get_state(IdBt2),
                                            ops=lists:reverse(Log),
                                            ts=Ts}},
    {ok, LogBt2} = cbt_btree:add(LogBt, [Transaction]),
    {ok, Db#db{by_id=IdBt2, log=LogBt2}};
run_transaction([{add, Key, Value} | Rest], {ToAdd, ToRem}, Log, TransactId,
                Ts, #db{fd=Fd}=Db) ->
    %% we are storing the value directly in the file, the btree will
    %% only keep a reference so we don't have the value multiple time.
    {ok, Pos, Size} = cbt_file:append_term_crc32(Fd, Value),
    Value1 =  {Key, {Pos, Size}, TransactId, Ts},
    run_transaction(Rest, {[{Key, Value1} | ToAdd], ToRem},
                    [{add, Value1} | Log],  TransactId, Ts, Db);
run_transaction([{remove, Key} | Rest], {ToAdd, ToRem}, Log, TransactId, Ts, Db) ->
    %% remove a key
    run_transaction(Rest, {ToAdd, [Key | ToRem]}, [{remove, Key} | Log],
                    TransactId, Ts, Db);
run_transaction([{fn, Func} | Rest], AddRemove, Log, TransactId, Ts, Db) ->
    %% execute a transaction function
    case cowdb_util:apply(Func, [Db]) of
        {error, _Reason}=Error ->
            Error;
        cancel ->
            {error, canceled};
        Ops ->
            run_transaction(Ops ++ Rest, AddRemove, Log, TransactId, Ts, Db)
    end;
run_transaction(_, _, _, _, _, _) ->
    {error, unknown_op}.

%% execute transaction
do_transaction(Fun, TransactId) ->
    erlang:put(cowdb_trans, TransactId),
    try
        case catch Fun() of
            {ok, Db} ->
                cowdb_util:commit_transaction(TransactId, Db);
            Error ->
                Error
        end
    after
        erlang:erase(cowdb_trans)
    end.

call_init(Fun, OldTransactId, NewTransactId, Db) ->
    cowdb_util:apply(Fun, [Db, OldTransactId, NewTransactId]).


req(Proc, R) ->
    Ref = erlang:monitor(process, Proc),
    Proc ! ?COWDB_CALL(self(), R),
    receive
        {'DOWN', Ref, process, Proc, _Info} ->
            badarg;
        {Proc, Reply} ->
            erlang:demonitor(Ref, [flush]),
            Reply
    end.

maybe_compact(#db{auto_compact=false}) ->
    false;
maybe_compact(#db{fd=Fd, compact_limit=Limit}) ->
    Size = cbt_file:bytes(Fd),
    Size >= Limit.
