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
-export([init_store/2, init_store/3,
         open_store/2,
         delete_store/2,
         stores/1]).

%% gen server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).


-include("cowdb.hrl").

-type db() :: #db{} | pid().
-export_type([db/0]).

-type storeid() :: term().
-opaque store() :: {db(), storeid()} | {db(), #btree{}}.
-export_type([storeid/0, store/0]).

open(FilePath, InitFunc) ->
    open(FilePath, InitFunc, []).

open(FilePath, InitFunc, Options) ->
    SpawnOpts = cowdb_util:get_opt(spawn_opts, Options, []),
    gen_server:start(?MODULE, [FilePath, InitFunc, Options], SpawnOpts).



open_link(FilePath, InitFunc) ->
    open_link(FilePath, InitFunc, []).

open_link(FilePath, InitFunc, Options) ->
    SpawnOpts = cowdb_util:get_opt(spawn_opts, Options, []),
    gen_server:start_link(?MODULE, [FilePath, InitFunc, Options], SpawnOpts).


%% @doc initialise a store, it can only happen in a version_change
%% transactions and be used in `init/1' or `upgrade/2' functions of the
%% database module.
-spec init_store(db(), storeid()) ->
    {ok, store(), db()}
    | store_already_defined
    | {error, term()}.
init_store(Db, StoreId) ->
    init_store(Db, StoreId, []).

%% @doc initialise a store, it can only happen in a version_change
%% transactions and be used in `init/1' or `upgrade/2' functions of the
%% database module.
-spec init_store(db(), storeid(), cowdb_store:store_options()) ->
    {ok, store(), db()}
    | store_already_defined
    | {error, term()}.
init_store(Db, StoreId, Options) ->
    cowdb_store:init(Db, StoreId, Options).

%% @doc open a store to be used in transactions. When open in an update
%% transaction function you will only be able to use it to query the
%% data.
-spec open_store(cowdb:db(), cowdb:storeid()) ->
    {ok, cowdb:store()}
    | undefined.
open_store(Db, StoreId) ->
    cowdb_store:open(Db, StoreId).

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

%% --------------------
%% gen_server callbacks
%% --------------------


init([FilePath, InitFunc, Options]) ->
     %% set openoptions
    OpenOptions = case proplists:get_value(override, Options, false) of
        true -> [create_if_missing, override];
        false -> [create_if_missing]
    end,

    case cowdb_file:open(FilePath, OpenOptions) of
        {ok, Fd} ->
            {ok, UpdaterPid} = cowdb_updater:start_link(self(), Fd,
                                                        FilePath, InitFunc,
                                                        Options),
            Db = cowdb_updater:get_db(UpdaterPid),
            process_flag(trap_exit, true),
            {ok, Db};
        Error ->
            Error
    end.

handle_call(get_db, _From, Db) ->
    {reply, Db, Db};

handle_call(stores, _From, #db{stores=Stores}=Db) ->
    Names = [K || {K, _Store} <- Stores],
    {reply, Names, Db};


handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
