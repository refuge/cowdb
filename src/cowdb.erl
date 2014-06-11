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


-export([open/2, open/3, open_link/2, open_link/3]).
-export([close/1]).
-export([set_metadata/3,
         get_metadata/1, get_metadata/2, get_metadata/3,
         delete_metadata/2]).


-export([open_store/2, open_store/3,
         delete_store/2,
         stores/1,
         count/1, count/2]).

-export([get/2, get/3,
         lookup/2, lookup/3,
         fold/3, fold/4, fold/5,
         fold_reduce/5,
         put/2, put/3,
         delete/2, delete/3,
         add/2, add/3,
         remove/2, remove/3,
         add_remove/3, add_remove/4,
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
-spec open(FilePath::string(), InitFunc::function()) ->
    {ok, Db::pid()}
    | {error, term()}.
open(FilePath, InitFunc) ->
    open(FilePath, InitFunc, []).


%% @doc open a cowdb database, pass a function to initialise the stores and
%% indexes.
-spec open(FilePath::string(), InitFunc::function(), Option::list()) ->
    {ok, Db::pid()}
    | {error, term()}.
open(FilePath, InitFunc, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start(?MODULE, [FilePath, InitFunc, Options], SpawnOpts).


%% @doc open a cowdb databas as part of the supervision treee, pass a
%% function to initialise the stores and indexes.
-spec open_link(FilePath::string(), InitFunc::function()) ->
    {ok, Db::pid()}
    | {error, term()}.

open_link(FilePath, InitFunc) ->
    open_link(FilePath, InitFunc, []).


%% @doc open a cowdb database as art of the supervision tree, pass a
%% function to initialise the stores and indexes.
-spec open_link(FilePath::string(), InitFunc::function(), Option::list()) ->
    {ok, Db::pid()}
    | {error, term()}.

open_link(FilePath, InitFunc, Options) ->
    SpawnOpts = cbt_util:get_opt(spawn_opts, Options, []),
    gen_server:start_link(?MODULE, [FilePath, InitFunc, Options], SpawnOpts).


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


set_metadata(Ref, Key, Value) ->
    transact(Ref, [{set_meta, Key, Value}]).



get_metadata(DbPid) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    get_metadata(Db);
get_metadata(#db{meta=Meta}) ->
    Meta.

get_metadata(Db, Key) ->
    get_metadata(Db, Key, undefined).

get_metadata(DbPid, Key, Default) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    get_metadata(Db, Key, Default);
get_metadata(#db{meta=Meta}, Key, Default) ->
    proplists:get_value(Key, Meta, Default).

delete_metadata(DbPid, Key) ->
    transact(DbPid, [{delete_meta, Key}]).




%% @doc initialise a store, it can only happen in a version_change
%% transactions and be used in `init/1' or `upgrade/2' functions of the
%% database module.
%% %% Options:
%% <ul>
%% <li> `{split, fun(Btree, Value)}' : Take a value and extract content if
%% needed from it. It returns a {key, Value} tuple. You don't need to
%% set such function if you already give a {Key, Value} tuple to your
%% add/add_remove functions.</li>
%% <li>`{join, fun(Key, Value)'} : The fonction takes the key and value and
%% return a new Value ussed when you lookup. By default it return a
%% {Key, Value} .</li>
%% <li>`{reduce_fun, ReduceFun'} : pass the reduce fun</li>
%% <li>`{compression, nonde | snappy}': the compression methods used to
%% compress the data</li>
%% <li>`{less, LessFun(KeyA, KeyB)}': function used to order the btree that
%% compare two keys</li>
%% </ul>
-spec open_store(db(), storeid()) ->
    {ok, store(), db()}
    | store_already_defined
    | {error, term()}.
open_store(Db, StoreId) ->
    open_store(Db, StoreId, []).

%% @doc initialise a store, it can only happen in a version_change
%% transactions and be used in `init/1' or `upgrade/2' functions of the
%% database module.
%%
%% Store options:
-spec open_store(db(), storeid(), cowdb_store:store_options()) ->
    {ok, store(), db()}
    | store_already_defined
    | {error, term()}.
open_store(Db, StoreId, Options) ->
    cowdb_store:open(Db, StoreId, Options).

%% delete a store in the database. It can only happen on a version
%% change transaction.
%%
%% Warning: deleting a store only remove the reference to it in the
%% database. Data will be removed during compaction.
-spec delete_store(db(), cowdb:storeid()) -> {ok, cowdb:db()}.
delete_store(Db, StoreId) ->
    cowdb_store:delete(Db, StoreId).

%% @doc list stores name
-spec stores(pid()) -> [term()].
stores(Db) ->
    gen_server:call(Db, stores).

%% @doc get the number of objects stored in the database.
-spec count(store()) -> {ok, integer()} | {error, term()}.
count({Ref, StoreId}) ->
    count(Ref, StoreId).


%% @doc get the number of objects stored in the database.
-spec count(db(), storeid()) -> {ok, integer()} | {error, term()}.
count(DbPid, StoreId) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    count(Db, StoreId);
count(#db{stores=Stores}, StoreId) ->
    case lists:keyfind(StoreId, 1, Stores) of
        false -> unknown_store;
        {StoreId, Store} ->
            case cbt_btree:full_reduce(Store) of
                {ok, {Count, _}} -> {ok, Count};
                {ok, Count} -> {ok, Count}
            end
    end.

%% @doc get an object from its key
-spec get(store(), Key::any()) -> {ok, any()} | {error, term()}.
get({Ref, StoreId}, Key) ->
    get(Ref, StoreId, Key).

get(Ref, StoreId, Key) ->
    [Val] = lookup(Ref, StoreId, [Key]),
    Val.

%% @doc get a list of object from theyir key
-spec lookup(store(), Keys::[any()]) -> {ok, any()} | {error, term()}.
lookup({Ref, StoreId}, Keys) ->
    lookup(Ref, StoreId, Keys).

lookup(DbPid, StoreId, Keys) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    lookup(Db, StoreId, Keys);
lookup(#db{reader_fd=Fd, stores=Stores}, StoreId, Keys) ->
    case lists:keyfind(StoreId, 1, Stores) of
        false -> unknown_store;
        {StoreId, Store} ->
            cbt_btree:lookup(Store#btree{fd=Fd}, Keys)
    end.

%% @doc fold all objects form the dabase
fold({Ref, StoreId}, Fun, Acc) ->
    fold(Ref, StoreId, Fun, Acc, []).


fold(DbPid, StoreId, Fun, Acc) when is_pid(DbPid) ->
    fold(DbPid, StoreId, Fun, Acc, []);
fold({Ref, StoreId}, Fun, Acc, Options) ->
    fold(Ref, StoreId, Fun, Acc, Options).



%% @doc fold all objects form the database with range options
fold(DbPid, StoreId, Fun, Acc, Options) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    fold(Db, StoreId, Fun, Acc, Options);
fold(#db{reader_fd=Fd, stores=Stores}, StoreId, Fun, Acc, Options) ->
    case lists:keyfind(StoreId, 1, Stores) of
        false -> unknown_store;
        {StoreId, Store} ->
            cbt_btree:fold(Store#btree{fd=Fd}, Fun, Acc, Options)
    end.


%% @doc fold the reduce function over the results.
fold_reduce(DbPid, StoreId, ReduceFun, Acc, Options) when is_pid(DbPid) ->
    Db = gen_server:call(DbPid, get_db, infinity),
    fold_reduce(Db, StoreId,  ReduceFun, Acc, Options);
fold_reduce(#db{reader_fd=Fd, stores=Stores}, StoreId, ReduceFun0, Acc,
            Options) ->
    case lists:keyfind(StoreId, 1, Stores) of
        false ->
            unknown_store;
        {StoreId, Store} ->
            ReduceFun = fun(reduce, KVs) ->
                    Result = ReduceFun0(reduce, KVs),
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
            couch_btree:fold_reduce(Store#btree{fd=Fd}, WrapperFun, Acc,
                                    Options)
    end.


%% @doc add one object to a store
put({Db, StoreId}, Obj) ->
    put(Db, StoreId, Obj).

put(Db, StoreId, Obj) ->
    add(Db, StoreId, [Obj]).

%% @delete one object from the store
delete({Db, StoreId}, Obj) ->
    delete(Db, StoreId, Obj).

delete(Db, StoreId, Obj) ->
    remove(Db, StoreId, [Obj]).

%% @doc add  multiple objects to the database
add({Ref, StoreId}, ToAdd) ->
    add(Ref, StoreId, ToAdd).

add(Ref, StoreId, ToAdd) ->
    transact(Ref, [{add, StoreId, ToAdd}]).

%% @doc remove multiple objects from the database
remove({Ref, StoreId}, ToRem) ->
    remove(Ref, StoreId, ToRem).

remove(Ref, StoreId, ToRem) ->
    transact(Ref, [{remove, StoreId, ToRem}]).


%% @doc add and remove multiple objects at once from the database
add_remove({Ref, StoreId}, ToAdd, ToRem) ->
    add_remove(Ref, StoreId, ToAdd, ToRem).

add_remove(Ref, StoreId, ToAdd, ToRem) ->
    transact(Ref, [{add_remove, StoreId, ToAdd, ToRem}]).




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
init([FilePath, InitFunc, Options]) ->
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
                                                        FilePath, InitFunc,
                                                        Options),
            {ok, Db} = cowdb_updater:get_db(UpdaterPid),
            process_flag(trap_exit, true),
            {ok, Db};
        Error ->
            Error
    end.

%% @private
handle_call(get_db, _From, Db) ->
    {reply, Db, Db};

handle_call(stores, _From, #db{stores=Stores}=Db) ->
    Names = [K || {K, _Store} <- Stores],
    {reply, Names, Db};

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
