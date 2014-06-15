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

-module(cowdb_updater).

%% PUBLIC API
-export([start_link/5]).
-export([transact/3]).
-export([compact/2]).
-export([get_db/1]).
-export([call/3, call/4]).


%% proc lib export
-export([init/5]).

%% compaction util export
-export([init_db/6]).


-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").

-type trans_type() :: version_change | update.
-export_type([trans_type/0]).

start_link(DbPid, Fd, ReaderFd, FilePath, Options) ->
    proc_lib:start_link(?MODULE, init, [DbPid, Fd, ReaderFd, FilePath,
                                        Options]).


%% @doc get latest db state.
-spec get_db(pid()) -> cowdb:db().
get_db(Pid) ->
    do_call(Pid, get_db, [], infinity).

%% @doc send a transaction and wait for its result.
transact(UpdaterPid, Ops, Timeout) ->
    do_call(UpdaterPid, transact, {Ops, Timeout}, infinity).


compact(UpdaterPid, Options) ->
    do_call(UpdaterPid, start_compact, Options, infinity).


call(UpdaterPid, Label, Args) ->
    call(UpdaterPid, Label, Args, infinity).

call(UpdaterPid, Label, Args, Timeout) ->
    do_call(UpdaterPid, Label, Args, Timeout).


init(DbPid, Fd, ReaderFd, FilePath, Options) ->
    Header = case cbt_file:read_header(Fd) of
        {ok, Header1, _Pos} ->
            Header1;
        no_valid_header ->
            Header1 = #db_header{},
            {ok, _} = cbt_file:write_header(Fd, Header1),
            Header1
    end,


    case catch init_db(Header, DbPid, Fd, ReaderFd, FilePath,
                          Options) of
        {ok, Db} ->
            proc_lib:init_ack({ok, self()}),
            loop(Db);
        Error ->
            error_logger:info_msg("error initialising the database ~p: ~p",
                                   [FilePath, Error]),
            proc_lib:init_ack(Error)
    end.


loop(#db{tid=LastTid, db_pid=DbPid, compactor_info=CompactInfo,
         file_path=Path}=Db) ->
    receive
        {?MODULE, transact, {Tag, Client}, {OPs, Options}} ->
            TransactId = LastTid + 1,
            {Reply, Db2} = case catch handle_transaction(TransactId, OPs,
                                                          Options, Db) of
                {ok, Db1} ->
                    ok = gen_server:call(DbPid, {db_updated, Db1},
                                         infinity),
                    {{ok, TransactId}, Db1};
                Error ->
                    {Error, Db}
            end,

            Client ! {Tag, Reply},
            loop(Db2);

        {?MODULE, start_compact, {Tag, Client}, Options} ->
            case Db#db.compactor_info of
                nil ->
                    Pid = spawn_link(fun() ->
                                    cowdb_compaction:start(Db, Options)
                            end),
                    Db2 = Db#db{compactor_info=Pid},
                    ok = gen_server:call(DbPid, {db_updated, Db2},
                                         infinity),
                    Client ! {Tag, ok},
                    loop(Db2);
                _CompactorPid ->
                    Client ! {Tag, ok},
                    loop(Db)
            end;
        {?MODULE, compact_done, {_Tag, _Client}, _CompactFile}
                when CompactInfo =:= nil ->
            %% the compactor already exited
            loop(Db);
        {?MODULE, compact_done, {Tag, Client}, CompactFile} ->
            #db{fd=Fd, reader_fd=ReaderFd, file_path=FilePath}=Db,
            {ok, NewFd} = cbt_file:open(CompactFile),
            {ok, NewReaderFd} = cbt_file:open(CompactFile, [read_only]),
            {ok, NewHeader, _Pos} = cbt_file:read_header(NewFd),

            #db{tid=Tid} = NewDb = cowdb_util:init_db(NewHeader, Db#db.db_pid,
                                                      NewFd, NewReaderFd,
                                                      FilePath,
                                                      Db#db.options),

            unlink(NewFd),
            unlink(NewReaderFd),

            case Db#db.tid == Tid of
                true ->
                    %% send to the db the new value
                    ok = gen_server:call(DbPid, {db_updated, NewDb},
                                         infinity),

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
                    Client ! {Tag, ok},
                    loop(NewDb);
                false ->
                    cbt_file:close(NewFd),
                    cbt_file:close(NewReaderFd),
                    Client ! {Tag, {retry, Db}}
            end;
        {?MODULE, cancel_compact, {Tag, Client}, []} ->
            Db2 = case Db#db.compactor_info of
                nil ->
                    Db;
                CompactorPid ->
                    cowdb_util:shutdown_sync(CompactorPid),
                    cowdb_compaction:delete_compact_file(Db),
                    Db#db{compactor_info=nil}
            end,
            Client ! {Tag, ok},
            loop(Db2);
        {?MODULE, get_db, {Tag, Client}, []} ->
            Client ! {Tag, {ok, Db}},
            loop(Db);
        Msg ->
            error_logger:error_msg(
                "cow updater of ~p received unexpected message ~p~n",
                [Path, Msg])
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
            %% initialise the database with the init function.
            do_transaction(fun() ->
                        case call_init(InitFunc, Tid, TransactId, Db) of
                            {ok, Ops} ->
                                %% an init function can return an initial
                                %% transaction.
                                run_transaction(Ops, {[], []}, TransactId,
                                                Db);
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
                                    run_transaction(OPs, {[], []},
                                                    TransactId, Db)
                            end, TransactId),

                    UpdaterPid ! {TransactId, {done, Resp}}
            end),

    %% wait for its resilut
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

%% roll back t the latest root
%% we don't try to edit in place instead we take the
%% latest known header and append it to the database file
rollback_transaction(#db{fd=Fd, header=Header}) ->
    ok= cbt_file:sync(Fd),
    {ok, _Pos} = cbt_file:write_header(Fd, Header),
    ok= cbt_file:sync(Fd),
    ok.

run_transaction([], {ToAdd, ToRem}, TransactId,
                #db{by_id=IdBt, log=LogBt}=Db) ->
    %% we atomically store the transaction.
    {ok, Found, IdBt2} = cbt_btree:query_modify(IdBt, ToRem, ToAdd, ToRem),


    %% reconstruct transactions operations for the log
    Ops0 = lists:foldl(fun({_K, V}, Acc) ->
                    [{add, V} | Acc]
            end, [], ToAdd),
    Ops = lists:reverse(lists:foldl(fun
                    ({ok, {_K, {Key, Pointer, _, Ts}}}, Acc) ->
                        V1 = {Key, Pointer, TransactId, Ts},
                        [{remove, V1} | Acc];
                    ({not_found, _}, Acc) ->
                        Acc
                end, Ops0, Found)),
    %% store the new log
    Transaction = {TransactId, #transaction{tid=TransactId,
                                            by_id=cbt_btree:get_state(IdBt2),
                                            ops=Ops,
                                            ts=cowdb_util:timestamp()}},
    {ok, LogBt2} = cbt_btree:add(LogBt, [Transaction]),
    {ok, Db#db{by_id=IdBt2, log=LogBt2}};
run_transaction([{add, Key, Value} | Rest], {ToAdd, ToRem}, TransactId,
                #db{fd=Fd}=Db) ->
    %% we are storing the value directly in the file, the btrees will
    %% only keep a reference so we don't have the value multiple time.
    {ok, Pos, Size} = cbt_file:append_term_crc32(Fd, Value),
    Ts = cowdb_util:timestamp(),
    Value1 =  {Key, {Pos, Size}, TransactId, Ts},
    run_transaction(Rest, {[{Key, Value1} | ToAdd], ToRem}, TransactId, Db);
run_transaction([{remove, Key} | Rest], {ToAdd, ToRem}, TransactId, Db) ->
    %% remove a key
    run_transaction(Rest, {ToAdd, [Key | ToRem]}, TransactId, Db);
run_transaction([{fn, Func} | Rest], AddRemove, TransactId, Db) ->
    %% execute a transaction function
    Ops = cowdb_util:apply(Func, [Db]),
    run_transaction(Ops ++ Rest, AddRemove, TransactId, Db);
run_transaction(_, _, _, _) ->
    {error, unknown_op}.

%% execute transactoin
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

do_call(UpdaterPid, Label, Request, Timeout) ->
    Tag = erlang:monitor(process, UpdaterPid),
    catch erlang:send(UpdaterPid, {?MODULE, Label, {Tag, self()}, Request},
                      [noconnect]),
    receive
        {Tag, Reply} ->
            erlang:demonitor(Tag, [flush]),
            Reply;
        {'DOWN', Tag, _, _, noconnection} ->
			exit({nodedown, node(UpdaterPid)});
		{'DOWN', Tag, _, _, Reason} ->
			exit(Reason)
    after Timeout ->
            erlang:demonitor(Tag, [flush]),
            exit(timeout)
    end.
