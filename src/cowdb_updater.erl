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
-export([start_link/6]).
-export([transaction_type/0]).
-export([transact/3]).
-export([get_db/1]).

%% proc lib export
-export([init/6]).



-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").

-type trans_type() :: version_change | update.
-export_type([trans_type/0]).




start_link(DbPid, Fd, ReaderFd, FilePath, InitFunc, Options) ->
    proc_lib:start_link(?MODULE, init, [DbPid, Fd, ReaderFd, FilePath,
                                        InitFunc, Options]).


%% @doc get current transaction type
-spec transaction_type() -> trans_type().
transaction_type() ->
    erlang:get(cowdb_trans).

%% @doc get latest db state.
-spec get_db(pid()) -> cowdb:db().
get_db(Pid) ->
    do_call(Pid, get_db, [], infinity).


transact(UpdaterPid, Ops, Timeout) ->
    TransactId = make_ref(),
    do_call(UpdaterPid, transact, {TransactId, Ops, Timeout}, infinity).

init(DbPid, Fd, ReaderFd, FilePath, InitFunc, Options) ->
    Header = case cbt_file:read_header(Fd) of
        {ok, Header1, _Pos} ->
            Header1;
        no_valid_header ->
            Header1 = #db_header{},
            {ok, _} = cbt_file:write_header(Fd, Header1),
            Header1
    end,

    case catch init_db(Header, DbPid, Fd, ReaderFd, FilePath, InitFunc,
                       Options) of
        {ok, Db} ->
            proc_lib:init_ack({ok, self()}),
            loop(Db);
        Error ->
            error_logger:info_msg("error initialising the database ~p: ~p",
                                   [FilePath, Error]),
            proc_lib:init_ack(Error)
    end.


loop(#db{db_pid=DbPid, file_path=Path}=Db) ->
    receive
        {?MODULE, transact, {Tag, Client}, {TransactId, OPs, Options}} ->
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


handle_transaction(TransactId, OPs, Timeout, Db) ->
    UpdaterPid = self(),

    %% spawn a transaction so we can cancel it if needed.
    TransactPid = spawn(fun() ->
                    %% execute the transaction
                    Resp = do_transaction(fun() ->
                                    run_transaction(OPs, Db)
                            end, update),

                    UpdaterPid ! {TransactId, {done, Resp}}
            end),

    %% wait for its resilut
    receive
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


init_db(Header, DbPid, Fd, ReaderFd, FilePath, InitFunc, Options) ->
    NewVersion = proplists:get_value(db_version, Options, 1),
    DefaultFSyncOptions = [before_header, after_header, on_file_open],
    FSyncOptions = cbt_util:get_opt(fsync_options, Options,
                                      DefaultFSyncOptions),

    ok = maybe_sync(on_file_open, Fd, FSyncOptions),

    #db_header{db_version=OldVersion,
               root=RootP,
               meta=Meta} = Header,

    {ok, Root} = cbt_btree:open(RootP, Fd),

    Stores = case RootP of
        nil -> [];
        _ ->
            {ok, _, Stores1} = cbt_btree:fold(Root, fun({Id, P}, Acc) ->
                            {ok, [{Id, P} | Acc]}
                    end, []),
            Stores1
    end,

    Db0 = #db{version =NewVersion,
              db_pid=DbPid,
              updater_pid=self(),
              fd=Fd,
              reader_fd=ReaderFd,
              root=Root,
              meta=Meta,
              stores=lists:reverse(Stores),
              header=Header,
              file_path=FilePath,
              fsync_options=FSyncOptions},


    %% retrieve the initialisation status, check if the database need to
    %% be upgraded.
    InitStatus = case {OldVersion, RootP} of
        {NewVersion, nil} ->
            init;
        {NewVersion, _} ->
            current;
        _ ->
            {upgrade, NewVersion, OldVersion}
    end,


    %% initialise the database with the init function.
    do_transaction(fun() ->
                    case call_init(InitFunc, InitStatus, Db0) of
                        {ok, Db2} ->
                            {ok, Db2};
                        {ok, Db2, Ops} ->
                            %% an init function can return an initial
                            %% transaction.
                            run_transaction(Ops, Db2);
                        Error ->
                            Error
                    end
            end, version_change).

run_transaction([], Db) ->
    {ok, Db};
run_transaction([{set_meta, Key, Value} | Rest], #db{meta=Meta}=Db) ->
    Meta2 = cowdb_util:set_property(Key, Value, Meta),
    run_transaction(Rest, Db#db{meta=Meta2});
run_transaction([{delete_meta, Key} | Rest], #db{meta=Meta}=Db) ->
    Meta2 = cowdb_util:delete_property(Key, Meta),
    run_transaction(Rest, Db#db{meta=Meta2});
run_transaction([{add, StoreId, ToAdd} | Rest], Db) ->
    %% add a value
    {ok, _, Db2} = query_modify(StoreId, Db, [], ToAdd, []),
    run_transaction(Rest, Db2);
run_transaction([{remove, StoreId, ToRem} | Rest],  Db) ->
    %% remove a key
    {ok, _, Db2} = query_modify(StoreId, Db, [], [], ToRem),
    run_transaction(Rest, Db2);
run_transaction([{add_remove, StoreId, ToAdd, ToRem} | Rest], Db) ->
    %% add remove keys
    {ok, _, Db2} = query_modify(StoreId, Db, [], ToAdd, ToRem),
    run_transaction(Rest, Db2);
run_transaction([{query_modify, StoreId, ToFind, ToAdd, ToRem, Func} | Rest],
                Db) ->
    {ok, Found, Db2} = query_modify(StoreId, Db, ToFind, ToAdd, ToRem),
    Ops = cowdb_util:apply(Func, [Db, Found]),
    {ok, Db2} = run_transaction(Ops, Db),
    run_transaction(Rest, Db2);
run_transaction([{fn, Func} | Rest], Db) ->
    %% execute a transaction function
    Ops = cowdb_util:apply(Func, [Db]),
    {ok, Db2} = run_transaction(Ops, Db),
    run_transaction(Rest, Db2);
run_transaction(_, _) ->
    {error, unknown_op}.

%% execute transactoin
do_transaction(Fun, Status) ->
    erlang:put(cowdb_trans, Status),
    try
        case catch Fun() of
            {ok, Db} ->
                commit_transaction(Status, Db);
            Error ->
                Error
        end
    after
        erlang:erase(cowdb_trans)
    end.

%% TODO: improve the transacton commit to make it faster.
commit_transaction(version_change, #db{root=Root, meta=Meta, stores=Stores,
                                       old_stores=OldStores,
                                       header=OldHeader}=Db) ->

    %% update the root tree
    ToRemove = lists:foldl(fun({K, _P}, Acc) ->
                    case lists:keyfind(K, 1, Stores) of
                        false -> [K |Acc];
                        _ -> Acc
                    end
            end, [], OldStores),
    ToAdd = [{K, cbt_btree:get_state(Btree)} || {K, Btree} <- Stores],
    {ok, Root2} = cbt_btree:add_remove(Root, ToAdd, ToRemove),

    %% commit the transactions
    NewHeader = OldHeader#db_header{root=cbt_btree:get_state(Root2),
                                    meta=Meta},
    ok = write_header(NewHeader, Db),

    %% return the new db
    {ok, Db#db{root=Root2, meta=Meta, header=NewHeader, old_stores=ToAdd}};
commit_transaction(_,  #db{root=Root, meta=Meta, stores=Stores,
                           old_stores = OldStores,
                           header=OldHeader}=Db) ->

    %% look at updated root to only store their changes
    ToAdd0 = [{K, cbt_btree:get_state(Btree)} || {K, Btree} <- Stores],
    ToAdd = ToAdd0 -- OldStores,
    %% store the new root
    {ok, Root2} = cbt_btree:add_remove(Root, ToAdd, []),
    %% write the header
    NewHeader = OldHeader#db_header{root=cbt_btree:get_state(Root2),
                                    meta=Meta},
    ok = write_header(NewHeader, Db),
    {ok, Db#db{root=Root2, meta=Meta, header=NewHeader, old_stores=ToAdd0}}.


call_init(Fun, InitStatus, Db) ->
    cowdb_util:apply(Fun, [InitStatus, Db]).

query_modify(StoreId, Db, LookupKeys, InsertValues, RemoveKeys) ->
    case get_store(StoreId, Db) of
        {ok, Store} ->
            {ok, KVs, Store2} = cbt_btree:query_modify(Store, LookupKeys,
                                                       InsertValues,
                                                       RemoveKeys),
            Db2 = set_store(StoreId, Store2, Db),
            {ok, KVs, Db2};
        false ->
            {error, {unknown_store, StoreId}}
    end.

get_store(StoreId, #db{stores=Stores}) ->
    case lists:keyfind(StoreId, 1, Stores) of
        false -> false;
        {StoreId, Store} -> {ok, Store}
    end.

set_store(StoreId, Store,  #db{stores=[]}=Db) ->
    Db#db{stores=[{StoreId, Store}]};
set_store(StoreId, Store,  #db{stores=Stores}=Db) ->
    NStores = lists:keyreplace(StoreId, 1, Stores, {StoreId, Store}),
    Db#db{stores=NStores}.


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
