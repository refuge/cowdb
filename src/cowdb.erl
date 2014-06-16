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


-module(cowdb).
-behaviour(gen_server).


%% PUBLIC API
-export([open/1, open/2, open/3,
         open_link/1, open_link/2, open_link/3,
         close/1,
         db_info/1,
         count/1,
         data_size/1,
         get/2,
         lookup/2,
         put/3,
         delete/2,
         fold/3, fold/4,
         fold_reduce/4,
         transact/2, transact/3,
         log/4, log/5,
         get_snapshot/2,
         compact/1,
         cancel_compact/1]).


%% gen server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).


-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").

-type timeout() :: infinity | integer().
-export_type([timeout/0]).


-type compression_method() :: snappy | lz4 | gzip
                              | {deflate, Level::integer()} | none.
-type fsync_options() :: [before_header | after_header | on_file_open].
-type open_options() :: [{compression,compression_method()}
                         | {fsync_options, fsync_options()}
                         | auto_compact | {auto_compact, boolean()}
                         | {compact_limit, integer()}
                         | {reduce, fun()}
                         | {less, fun()}
                         | {init_func, fun()}].
-type mfa() :: {local, Name::atom()}
    | {global, GlobalName::term()}
    | {via, ViaName::term()}.

-export_type([compression_method/0,
              fsync_options/0,
              open_options/0,
              mfa/0]).

-type db() :: #db{} | pid().
-export_type([db/0]).

-type fold_options() :: [{dir, fwd | rev} | {start_key, term()} |
                         {end_key, term()} | {end_key_gt, term()} |
                         {key_group_fun, fun()}].

-type transact_fn() :: {module(), fun(), [any()]} |
                       {module(), fun()} |
                       fun().
-type transact_id() :: integer() | tx_end.
-type transact_ops() :: [{add, term(), any()} |
                         {remove, term()} |
                         {fn, transact_fn()}].
-export_type([transact_fn/0,
              transact_id/0,
              transact_ops/0]).

%% @doc open a cowdb database, pass a function to initialise the stores and
%% indexes.
-spec open(FilePath::string()) ->
    {ok, Db::pid()}
    | {error, term()}.
open(FilePath) ->
    open(FilePath, []).


%% @doc open a cowdb database, pass a function to initialise the stores and
%% indexes.
-spec open(FilePath::string(), Option::open_options()) ->
    {ok, Db::pid()}
    | {error, term()}.
open(FilePath, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start(?MODULE, [FilePath, Options], [{spawn_opts, SpawnOpts}]).


%% @doc Create or open a hanoidb store with a registered name.
- spec open(Name::mfa(), FilePath::string(), Option::open_options()) ->
    {ok, Db::pid()}
    | {error, term()}.
open(Name, FilePath, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start(Name, ?MODULE, [FilePath, Options],
                     [{spawn_opts, SpawnOpts}]).

%% @doc open a cowdb databas as part of the supervision treee, pass a
%% function to initialise the stores and indexes.
-spec open_link(FilePath::string()) ->
    {ok, Db::pid()}
    | {error, term()}.

open_link(FilePath) ->
    open_link(FilePath, []).

%% @doc open a cowdb database as part of the supervision tree
-spec open_link(FilePath::string(), Option::open_options()) ->
    {ok, Db::pid()}
    | {error, term()}.

open_link(FilePath, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start_link(?MODULE, [FilePath, Options],
                          [{spawn_opts, SpawnOpts}]).

%% @doc open a cowdb database as part of the supervision tree with a
%% registerd name
- spec open_link(Name::mfa(), FilePath::string(), Option::open_options()) ->
    {ok, Db::pid()}
    | {error, term()}.
open_link(Name, FilePath, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start_link(Name, ?MODULE, [FilePath, Options],
                          [{spawn_opts, SpawnOpts}]).

%% @doc Close the file.
-spec close(DbPid::pid()) -> ok.
close(DbPid) ->
    try
        gen_server:call(DbPid, close, infinity)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.

%% @doc display database infos
-spec db_info(db()) -> {ok, list()}.
db_info(DbPid) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    db_info(Db);
db_info(#db{tid=EndT, start_time=StartTime, fd=Fd, by_id=IdBt, log=LogBt,
            compactor_info=Compactor, file_path=FilePath,
            header=#db_header{version=Version}}) ->
    {ok, _, StartT} = cbt_btree:fold(LogBt, fun({TransactId, _}, _) ->
                {stop, TransactId}
        end, nil, []),
    {ok, TxCount} = cbt_btree:full_reduce(LogBt),
    {ok, DiskSize} = cbt_file:bytes(Fd),

    {ObjCount, DataSize} =  case cbt_btree:full_reduce(IdBt) of
        {ok, {Count, Size, _}} ->{Count, Size};
        {ok, {Count, Size}} -> {Count, Size}
    end,

    {ok, [{file_path, FilePath},
          {object_count, ObjCount},
          {tx_count, TxCount},
          {tx_start, StartT},
          {tx_end, EndT},
          {compact_running, Compactor/=nil},
          {disk_size, DiskSize},
          {data_size, DataSize},
          {start_time, StartTime},
          {db_version, Version}]}.

%% @doc get the number of objects stored in the database.
-spec count(db()) -> {ok, integer()} | {error, term()}.
count(DbPid) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    count(Db);
count(#db{by_id=IdBt}) ->
    case cbt_btree:full_reduce(IdBt) of
        {ok, {Count, _, _}} -> {ok, Count};
        {ok, {Count, _}} -> {ok, Count}
    end.

%% @doc get the number of objects stored in the database.
-spec data_size(db()) -> {ok, integer()} | {error, term()}.
data_size(DbPid) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    data_size(Db);
data_size(#db{by_id=IdBt, log=LogBt}) ->
    VSize = case cbt_btree:full_reduce(IdBt) of
        {ok, {_, Size, _}} -> Size;
        {ok, {_, Size}} -> Size
    end,
    TotalSize = lists:sum([VSize, cbt_btree:size(IdBt),
                           cbt_btree:size(LogBt)]),
    {ok, TotalSize}.

%% @doc get an object from its key
-spec get(Db::db(), Key::any()) -> {ok, any()} | {error, term()}.
get(Db, Key) ->
    [Val] = lookup(Db, [Key]),
    Val.

%% @doc get a list of object from theyir key
-spec lookup(Db::db(), Keys::[any()]) -> {ok, any()} | {error, term()}.
lookup(DbPid, Keys) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    lookup(Db, Keys);
lookup(#db{reader_fd=Fd, by_id=IdBt}, Keys) ->
    Results = cbt_btree:lookup(IdBt#btree{fd=Fd}, Keys),
    lists:foldr(fun
            ({ok, {Key, {_, {Pos, _}, _, _}}}, Acc) ->
                {ok, Val} = cbt_file:pread_term(Fd, Pos),
                [{ok, {Key, Val}} | Acc];
            (Else, Acc) ->
                [Else | Acc]
        end, [], Results).

%% @doc fold all objects form the database
-spec fold(db(), fun(), any()) -> {ok, any(), any()} | {error, term()}.
fold(DbPid, Fun, Acc) ->
    fold(DbPid, Fun, Acc, []).

%% @doc fold all objects form the database with range options
-spec fold(db(), fun(), any(), fold_options())
    -> {ok, any(), any()}
    | {error, term()}.
fold(DbPid, Fun, Acc, Options) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    fold(Db, Fun, Acc, Options);
fold(#db{reader_fd=Fd, by_id=IdBt}, Fun, Acc, Options) ->
    Wrapper = fun({Key, {_, {Pos, _}, _, _}}, Acc1) ->
            {ok, Val} = cbt_file:pread_term(Fd, Pos),
            Fun({Key, Val}, Acc1)
    end,
    cbt_btree:fold(IdBt#btree{fd=Fd}, Wrapper, Acc, Options).


%% @doc fold the reduce function over the results.
fold_reduce(DbPid, ReduceFun, Acc, Options) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    fold_reduce(Db, ReduceFun, Acc, Options);
fold_reduce(#db{reader_fd=Fd, by_id=IdBt}, ReduceFun0, Acc, Options) ->
    ReduceFun = fun(reduce, KVs) ->
            KVs1 = lists:fold(fun({K, {_, {Pos, _}, _}}, AccKVs) ->
                {ok, Val} = cbt_file:pread_term(Fd, Pos),
                [{K, Val} | AccKVs]
            end, [], KVs),
            Result = ReduceFun0(reduce, lists:reverse(KVs1)),
            {0, Result};
        (rereduce, Reds) ->
            UsrReds = [UsrRedsList || {_, UsrRedsList} <- Reds],
            Result = ReduceFun0(rereduce, UsrReds),
            {0, Result}
    end,

    WrapperFun = fun({GroupedKey, _}, PartialReds, Acc0) ->
            {_, Reds} = couch_btree:final_reduce(ReduceFun,
                                                 PartialReds),
            ReduceFun(GroupedKey, Reds, Acc0)
    end,
    couch_btree:fold_reduce(IdBt#btree{fd=Fd}, WrapperFun, Acc,
                            Options).

%% @doc add one object to a store
-spec put(db(), term(), any()) -> {ok, transact_id()} | {error, term()}.
put(DbPid, Key, Value) ->
    transact(DbPid, [{add, Key, Value}]).

%% @doc delete one object from the store
-spec delete(db(), term()) -> {ok, transact_id()} | {error, term()}.
delete(DbPid, Key) ->
    transact(DbPid, [{remove, Key}]).


%% @doc execute a transaction
%% A transaction received operations to execute as a list:
%% <ul>
%% <li>`{add, Key, Value}' to add an object</li>
%% <li>`{remove, Key}' to remove a value</li>
%%<li> `{fn, Func}' a transaction function. A transaction function
%%reveived the db value like it was at the beginning of the transaction
%%as an argument. It's possible to pass arguments to it. A transaction
%%function return a list of operations and can wuery/maniuplate
%%function. The list of operations returned can also contain a
%%function.</li>
%%</ul>
%%
-spec transact(db(), transact_ops()) ->
    {ok, transact_id()}
    | {error, term()}.
transact(Ref, OPs) ->
    transact(Ref, OPs, infinity).

-spec transact(db(), transact_ops(), timeout()) ->
    {ok, transact_id()}
    | {error, term()}.
transact(Ref, OPs, Timeout) ->
    UpdaterPid = gen_server:call(Ref, get_updater, infinity),
    cowdb_updater:transact(UpdaterPid, OPs, Timeout).

%% @doc compact the database file
-spec compact(db()) -> {ok, pid()} | {error, term()}.
compact(Ref) ->
    UpdaterPid = gen_server:call(Ref, get_updater, infinity),
    cowdb_updater:compact(UpdaterPid, []).

%% @doc cancel compaction
-spec cancel_compact(db()) -> ok.
cancel_compact(Ref) ->
     UpdaterPid = gen_server:call(Ref, get_updater, infinity),
    cowdb_updater:cancel_compact(UpdaterPid).

%% @doc fold the transaction log
-spec log(Db::db(), StartT::transact_id(), Function::fun(), Acc::any()) ->
    {ok, NbTransactions::integer(), Acc2::any()}
    | {error, term()}.
log(Db, StartT, Fun, Acc) ->
    log(Db, StartT, tx_end, Fun, Acc).

%% @doc fold the transaction log
%% Args:
%% <ul>
%% <li>`Db': the db value (in transaction function) or pid</li>
%% <li>`StartT': transaction ID to start from</li>
%% <li>`EndT': transaction ID to stop</li>
%% <li>`Fun': function collection log result:
%% ```
%% fun({TransactId, Op, {K,V}, Ts}, Acc) ->
%%      {ok, Acc2} | {stop, Acc2}
%%  end
%% '''
%% where TransactId is the transaction ID `Transactid' where the `OP'
%% (`add' or `remove') on the Key/Value pair `{K, V}' has been run on
%% the unix time `Ts'.</li>
%% <li>`Acc': initial value to pass to the function.</li>
%% </ul>
%% The function return the total number of transactions in the range and
%% the values collected during folding.
-spec log(Db::db(), StartT::transact_id(), EndT::transact_id(),
          Function::fun(), Acc::any()) ->
    {ok, NbTransactions::integer(), Acc2::any()}
    | {error, term()}.
log(DbPid, StartT, EndT, Fun, Acc) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    log(Db, StartT, EndT, Fun, Acc);
log(#db{tid=LastTid, reader_fd=Fd, log=LogBt}, StartT, EndT0, Fun, Acc) ->
    EndT = case EndT0 of
        tx_end -> LastTid;
        _ when EndT0 > LastTid -> LastTid;
        _ -> EndT0
    end,

    Wrapper = fun({_TransactId, #transaction{ops=Ops}}, Acc1) ->
            fold_log_ops(Ops, Fd, Fun, Acc1)
    end,
    {ok, {_, [Count]}, Result} = cbt_btree:fold(LogBt#btree{fd=Fd}, Wrapper,
                                                Acc, [{start_key,  StartT},
                                                      {end_key, EndT}]),
    {ok, Count, Result}.


%% fold operations in a transaction.
fold_log_ops([], _Fd, _Fun, Acc) ->
    {ok, Acc};
fold_log_ops([{Op, {Key, {Pos, _}, TransactId, Ts}} | Rest], Fd, Fun, Acc) ->
    {ok, Val} = cbt_file:pread_term(Fd, Pos),
    case Fun({TransactId, Op, {Key, Val}, Ts}, Acc) of
        {ok, Acc2} ->
            fold_log_ops(Rest, Fd, Fun, Acc2);
        {stop, Acc2} ->
            {stop, Acc2}
    end.

%% @doc get a snapshot of the database at some point.
-spec get_snapshot(db(), transact_id()) -> {ok, db()} | {error, term()}.
get_snapshot(DbPid, TransactId) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    get_snapshot(Db, TransactId);
get_snapshot(#db{tid=Tid}=Db, tx_end)->
    get_snapshot(Db, Tid);
get_snapshot(#db{log=LogBt, reader_fd=Fd, by_id=IdBt}=Db, TransactId) ->
    case cbt_btree:lookup(LogBt#btree{fd=Fd}, [TransactId]) of
        [not_found] ->
            {error, not_found};
        [{ok, {_TransactId, #transaction{by_id=SnapshotRoot}}}] ->
            IdSnapshot = IdBt#btree{root=SnapshotRoot},
            {ok, Db#db{by_id=IdSnapshot}}
    end.

%% --------------------
%% gen_server callbacks
%% --------------------

%% @private
init([FilePath, Options]) ->
     %% set openoptions
    OpenOptions = case proplists:get_value(override, Options, false) of
        true -> [create_if_missing, override];
        false -> [create_if_missing]
    end,

    case cbt_file:open(FilePath, OpenOptions) of
        {ok, Fd} ->
            %% open the the reader file
            {ok, ReaderFd} = cbt_file:open(FilePath, [read_only]),

            %% initialise the db updater process
            {ok, UpdaterPid} = cowdb_updater:start_link(self(), Fd, ReaderFd,
                                                        FilePath, Options),
            {ok, Db} = cowdb_updater:get_db(UpdaterPid),
            process_flag(trap_exit, true),
            {ok, Db};
        Error ->
            Error
    end.

%% @private
handle_call(get_db, _From, Db) ->
    {reply, Db, Db};

handle_call(get_updater, _From, #db{updater_pid=UpdaterPid}=Db) ->
    {reply, UpdaterPid, Db};


handle_call({db_updated, #db{fd=Fd, reader_fd=ReaderFd}=Db}, _From,
            #db{fd=Fd, reader_fd=ReaderFd}) ->
    {reply, ok, Db};

handle_call({db_updated, #db{fd=Fd, reader_fd=ReaderFd}=Db}, _From,
            #db{fd=OldFd, reader_fd=OldReaderFd}) ->

    %% unlink old fds
    unlink(OldFd),
    unlink(OldReaderFd),
    %% link new fds
    link(Fd),
    link(ReaderFd),

    {reply, ok, Db};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'EXIT', _, normal}, Db) ->
    {noreply, Db};
handle_info({'EXIT', _, Reason}, Db) ->
    {stop, Reason, Db};
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
terminate(_Reason, #db{updater_pid=UpdaterPid, fd=Fd, reader_fd=ReaderFd}) ->
    %% close the updater pid
    ok = cowdb_util:shutdown_sync(UpdaterPid),
    %% close file descriptors
    ok = cbt_file:close(Fd),
    ok = cbt_file:close(ReaderFd),
    ok.
