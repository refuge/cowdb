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
-export([transaction_type/0]).
-export([transact/3]).
-export([get_db/1]).

%% proc lib export
-export([init/5]).


-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").

-type trans_type() :: version_change | update.
-export_type([trans_type/0]).

start_link(DbPid, Fd, ReaderFd, FilePath, Options) ->
    proc_lib:start_link(?MODULE, init, [DbPid, Fd, ReaderFd, FilePath,
                                        Options]).


%% @doc get current transaction type
-spec transaction_type() -> trans_type().
transaction_type() ->
    erlang:get(cowdb_trans).

%% @doc get latest db state.
-spec get_db(pid()) -> cowdb:db().
get_db(Pid) ->
    do_call(Pid, get_db, [], infinity).

%% @doc send a transaction and wait for its result.
transact(UpdaterPid, Ops, Timeout) ->
    do_call(UpdaterPid, transact, {Ops, Timeout}, infinity).


init(DbPid, Fd, ReaderFd, FilePath, Options) ->
    Header = case cbt_file:read_header(Fd) of
        {ok, Header1, _Pos} ->
            Header1;
        no_valid_header ->
            Header1 = #db_header{},
            {ok, _} = cbt_file:write_header(Fd, Header1),
            Header1
    end,

    case catch init_db(Header, DbPid, Fd, ReaderFd, FilePath, Options) of
        {ok, Db} ->
            proc_lib:init_ack({ok, self()}),
            loop(Db);
        Error ->
            error_logger:info_msg("error initialising the database ~p: ~p",
                                   [FilePath, Error]),
            proc_lib:init_ack(Error)
    end.


loop(#db{tid=LastTid, db_pid=DbPid, file_path=Path}=Db) ->
    receive
        {?MODULE, transact, {Tag, Client}, {OPs, Options}} ->
            TransactId = LastTid + 1,
            {Reply, Db2} = case catch handle_transaction(TransactId, OPs,
                                                          Options, Db) of
                {ok, Db1} ->
                    ok = gen_server:call(DbPid, {db_updated, Db1},
                                         infinity),
                    {ok, Db1};
                Error ->
                    {Error, Db}
            end,

            Client ! {Tag, Reply},
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
    DefaultFSyncOptions = [before_header, after_header, on_file_open],
    FSyncOptions = cbt_util:get_opt(fsync_options, Options,
                                      DefaultFSyncOptions),

    %% maybe sync the header
    ok = maybe_sync(on_file_open, Fd, FSyncOptions),

    %% extract infos from the header
    #db_header{tid=Tid,
               by_id=IdP,
               log=LogP} = Header,


    %% initialise the btrees
    Compression = cbt_util:get_opt(compression, Options, ?DEFAULT_COMPRESSION),
    DefaultLess = fun(A, B) -> A < B end,
    Less = cbt_util:get_opt(less, Options, DefaultLess),
    Reduce = by_id_reduce(Options),

    {ok, IdBt} = cbt_btree:open(IdP, Fd, [{compression, Compression},
                                          {less, Less},
                                          {reduce, Reduce}]),

    {ok, LogBt} = cbt_btree:open(LogP, Fd, [{compression, Compression},
                                            {reduce, fun log_reduce/2}]),

    %% initial db record
    Db0 = #db{tid=Tid,
              db_pid=DbPid,
              updater_pid=self(),
              fd=Fd,
              reader_fd=ReaderFd,
              by_id=IdBt,
              log=LogBt,
              header=Header,
              file_path=FilePath,
              fsync_options=FSyncOptions},

    case proplists:get_value(init_func, Options) of
        undefined ->
            {ok, Db0};
        InitFunc ->
            TransactId = Tid + 1,
            %% initialise the database with the init function.
            do_transaction(fun() ->
                        case call_init(InitFunc, Db0) of
                            {ok, Ops} ->

                                %% an init function can return an initial
                                %% transaction.
                                run_transaction(Ops, {[], []}, TransactId,
                                                Db0);
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
            cbt_util:shutdown_sync(TransactPid),
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
                    [{TransactId, {add, V}} | Acc]
            end, [], ToAdd),
    Ops = lists:reverse(lists:foldl(fun(V, Acc) ->
                        [{TransactId, {remove, V}} | Acc]
                end, Ops0, Found)),
    %% store the new log
    {ok, LogBt2} = cbt_btree:add(LogBt, Ops),
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
                commit_transaction(TransactId, Db);
            Error ->
                Error
        end
    after
        erlang:erase(cowdb_trans)
    end.


commit_transaction(TransactId,#db{by_id=IdBt,
                                  log=LogBt,
                                  header=OldHeader}=Db) ->

    %% write the header
    NewHeader = OldHeader#db_header{tid=TransactId,
                                    by_id=cbt_btree:get_state(IdBt),
                                    log=cbt_btree:get_state(LogBt)},
    ok = write_header(NewHeader, Db),
    {ok, Db#db{tid=TransactId, header=NewHeader}}.


call_init(Fun, Db) ->
    cowdb_util:apply(Fun, [Db]).

write_header(Header, #db{fd=Fd, fsync_options=FsyncOptions}) ->
    ok = maybe_sync(before_header, Fd, FsyncOptions),
    {ok, _} = cbt_file:write_header(Fd, Header),
    ok = maybe_sync(after_headerr, Fd, FsyncOptions),
    ok.

maybe_sync(Status, Fd, FSyncOptions) ->
    case lists:member(Status, FSyncOptions) of
        true ->
            ok = cbt_file:sync(Fd),
            ok;
        _ ->
            ok
    end.

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


%% TODO: make it native?
by_id_reduce(Options) ->
    case lists:keyfind(reduce, 1, Options) of
        false ->
            fun (reduce, KVs) ->
                    length(KVs);
                (rereduce, Reds) ->
                    lists:sum(Reds)
            end;
        {_, ReduceFun0} ->
            fun(reduce, KVs) ->
                    Count = length(KVs),
                    Result = ReduceFun0(reduce, KVs),
                    {Count, Result};
                (rereduce, Reds) ->
                    Count = lists:sum([Count0 || {Count0, _} <- Reds]),
                    UsrReds = [UsrRedsList || {_, UsrRedsList} <- Reds],
                    Result = ReduceFun0(rereduce, UsrReds),
                    {Count, Result}
            end
    end.


log_reduce(reduce, KVS) ->
    length(KVS);
log_reduce(rereduce, Reds) ->
    lists:sum(Reds).
