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
-behaviour(gen_server).


%% PUBLIC API
-export([start_link/5]).
-export([transaction_type/0]).
-export([get_db/1]).

%% gen server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).



-include("cowdb.hrl").

-type trans_type() :: version_change | update.
-export_type([trans_type/0]).

start_link(DbPid, Fd, FilePath, InitFunc, Options) ->
    gen_server:start_link(?MODULE, [DbPid, Fd, FilePath, InitFunc, Options],
                          []).


%% @doc get current transaction type
-spec transaction_type() -> trans_type().
transaction_type() ->
    erlang:get(cowdb_trans).

%% @doc get latest db state.
-spec get_db(pid()) -> cowdb:db().
get_db(Pid) ->
    gen_server:call(Pid, get_db, infinity).


init([DbPid, Fd, FilePath, InitFunc, Options]) ->
    Header = case cowdb_file:read_header(Fd) of
        {ok, Header1, _Pos} ->
            Header1;
        no_valid_header ->
            Header1 = #db_header{},
            {ok, _} = cowdb_file:write_header(Fd, Header1),
            Header1
    end,

    Db = init_db(Header, DbPid, Fd, FilePath, InitFunc, Options),
    {ok, Db}.

handle_call(get_db, _From, State) ->
    {reply, State, State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #db{fd=Fd, reader_fd=FdReader}) ->
    cowdb_file:close(Fd),
    cowdb_file:close(FdReader),
    ok.


init_db(Header, DbPid, Fd, FilePath, InitFunc, Options) ->
    NewVersion = proplists:get_value(db_version, Options, 1),
    DefaultFSyncOptions = [before_header, after_header, on_file_open],
    FSyncOptions = cowdb_util:get_opt(fsync_options, Options,
                                      DefaultFSyncOptions),

    case lists:member(on_file_open, FSyncOptions) of
        true ->
            ok = cowdb_file:sync(Fd);
        _ ->
            ok
    end,


    #db_header{db_version=OldVersion,
               root=RootP} = Header,

    {ok, Root} = cowdb_btree:open(RootP, Fd),

    Stores = case RootP of
        nil -> [];
        _ ->
            {ok, _, Stores1} = cowdb_btree:fold(Root, fun({Id, P}, Acc) ->
                            {ok, [{Id, P} | Acc]}
                    end, []),
            Stores1
    end,

    {ok, ReaderFd} = cowdb_file:open(FilePath, [read_only]),
    Db0 = #db{version =NewVersion,
              db_pid=DbPid,
              updater_pid=self(),
              fd=Fd,
              reader_fd=ReaderFd,
              root=Root,
              stores=lists:reverse(Stores),
              header=Header,
              file_path=FilePath},


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
    {ok, Db} = do_transaction(fun() ->
                    call_init(InitFunc, InitStatus, Db0)
            end, version_change),

    Db.

do_transaction(Fun, Status) ->
    erlang:put(cowdb_trans, Status),
    {ok, Db} = try
        Fun()
    after
        erlang:erase(cowdb_trans)
    end,
    commit_transaction(Status, Db).


%% TODO: improve the transacton commit to make it faster.
commit_transaction(version_change, #db{fd=Fd, root=Root, stores=Stores,
                                       old_stores=OldStores,
                                       header=OldHeader}=Db) ->

    %% update the root tree
    ToRemove = lists:foldl(fun({K, _P}, Acc) ->
                    case lists:keyfind(K, 1, Stores) of
                        false -> [K |Acc];
                        _ -> Acc
                    end
            end, [], OldStores),
    ToAdd = [{K, cowdb_btree:get_state(Btree)} || {K, Btree} <- Stores],
    {ok, Root2} = cowdb_btree:add_remove(Root, ToAdd, ToRemove),

    %% commit the transactions
    NewHeader = OldHeader#db_header{root=cowdb_btree:get_state(Root2)},
    {ok, _} = cowdb_file:write_header(Fd, NewHeader),

    %% return the new db
    {ok, Db#db{root=Root2, header=NewHeader, old_stores=ToAdd}};
commit_transaction(_,  #db{fd=Fd, root=Root, stores=Stores,
                            old_stores = OldStores,
                            header=OldHeader}=Db) ->

    %% look at updated root to only store their changes
    ToAdd0 = [{K, cowdb_btree:get_state(Btree)} || {K, Btree} <- Stores],
    ToAdd = ToAdd0 -- OldStores,
    %% store the new root
    {ok, Root2} = cowdb_btree:add_remove(Root, ToAdd, []),
    %% write the header
    NewHeader = OldHeader#db_header{root=cowdb_btree:get_state(Root2)},
    {ok, _} = cowdb_file:write_header(Fd, NewHeader),
    {ok, Db#db{root=Root2, header=NewHeader}}.


call_init({M, F, A}, InitStatus, Db) ->
    erlang:apply(M, F, [InitStatus, Db, A]);
call_init({M, F}, InitStatus, Db) ->
    M:F(InitStatus, Db, []);
call_init(InitFun, InitStatus, Db) ->
    InitFun(InitStatus, Db, []).
