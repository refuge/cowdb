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
-export([open/1, open/2,
         open_link/1, open_link/2,
         close/1,
         count/1,
         get/2,
         lookup/2,
         put/3,
         delete/2,
         fold/3, fold/4,
         fold_reduce/4,
         transact/2, transact/3]).


%% gen server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).


-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").

-type db() :: #db{} | pid().
-export_type([db/0]).

-type storeid() :: term().
-opaque store() :: {db(), storeid()} | {db(), #btree{}}.
-export_type([storeid/0, store/0]).

%% @doc open a cowdb database, pass a function to initialise the stores and
%% indexes.
-spec open(FilePath::string()) ->
    {ok, Db::pid()}
    | {error, term()}.
open(FilePath) ->
    open(FilePath, []).


%% @doc open a cowdb database, pass a function to initialise the stores and
%% indexes.
-spec open(FilePath::string(), Option::list()) ->
    {ok, Db::pid()}
    | {error, term()}.
open(FilePath, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start(?MODULE, [FilePath, Options], SpawnOpts).


%% @doc open a cowdb databas as part of the supervision treee, pass a
%% function to initialise the stores and indexes.
-spec open_link(FilePath::string()) ->
    {ok, Db::pid()}
    | {error, term()}.

open_link(FilePath) ->
    open_link(FilePath, []).


%% @doc open a cowdb database as art of the supervision tree, pass a
%% function to initialise the stores and indexes.
-spec open_link(FilePath::string(), Option::list()) ->
    {ok, Db::pid()}
    | {error, term()}.

open_link(FilePath, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start_link(?MODULE, [FilePath, Options], SpawnOpts).


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



%% @doc get the number of objects stored in the database.
-spec count(db()) -> {ok, integer()} | {error, term()}.
count(DbPid) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    count(Db);
count(#db{by_id=IdBt}) ->
    case cbt_btree:full_reduce(IdBt) of
        {ok, {Count, _}} -> {ok, Count};
        {ok, Count} -> {ok, Count}
    end.

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

%% @doc fold all objects form the dabase
fold(DbPid, Fun, Acc) ->
    fold(DbPid, Fun, Acc, []).

%% @doc fold all objects form the database with range options
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
put(DbPid, Key, Value) ->
    transact(DbPid, [{add, Key, Value}]).

%% @delete one object from the store
delete(DbPid, Key) ->
    transact(DbPid, [{remove, Key}]).


%% @doc execute a transaction
%% A transaction received operations to execute as a list:
%% <ul>
%% <li>`{add, StoreId, Obj}' to add an object</li>
%% <li>`{remove, StoreId, Key}' to remove a value</li>
%% <li> `{add_remove, StoreId, ToAdd, ToRemove}' to add and remove multiple keys and value at the same time</li>
%%<li> `{fn, Func}' a transaction function. A transaction function
%%reveived the db value like it was at the beginning of the transaction
%%as an argument. It's possible to pass arguments to it. A transaction
%%function return a list of operations and can wuery/maniuplate
%%function. The list of operations returned can also contain a
%%function.</li>
%%</ul>
%%
transact(Ref, OPs) ->
    transact(Ref, OPs, infinity).

transact(Ref, OPs, Timeout) ->
    UpdaterPid = gen_server:call(Ref, get_updater, infinity),
    cowdb_updater:transact(UpdaterPid, OPs, Timeout).

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

handle_call({db_updated, Db}, _From, _State) ->
    {reply, ok, Db};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
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
    ok = cbt_util:shutdown_sync(UpdaterPid),
    %% close file descriptors
    ok = cbt_file:close(Fd),
    ok = cbt_file:close(ReaderFd),
    ok.
