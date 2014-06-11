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

-module(cowdb_store).

-export([open/2, open/3]).
-export([delete/2]).

-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").


-type store_options() :: [{split, fun()} |
                          {join, fun()} |
                          {reduce, fun()} |
                          {compress, fun()} |
                          {less, fun()} |
                          {chunk_threshold, integer()}].
-export_type([store_options/0]).


%% @doc initialise a store, it can only happen in a version_change
%% transactions and be used in `init/1' or `upgrade/2' functions of the
%% database module.
-spec open(cowdb:db(), cowdb:storeid()) ->
    {ok, cowdb:store(), cowdb:db()}
    | store_already_defined
    | {error, term()}.
open(Db, StoreId) ->
    open(Db, StoreId, []).

%% @doc initialise a store, it can only happen in a version_change
%% transactions and be used in `init/1' or `upgrade/2' functions of the
%% database module.
-spec open(cowdb:db(), cowdb:storeid(), store_options()) ->
    {ok, cowdb:store(), cowdb:db()}
    | store_already_defined
    | {error, term()}.
open(#db{fd=Fd, stores=Stores}=Db, StoreId, Options) ->
    ?IF_TRANS(version_change, fun() ->
                case proplists:get_value(StoreId, Stores, nil) of
                    #btree{} ->
                        store_already_defined;
                    State ->
                        Options1 = wrap_reduce_fun(Options),
                        {ok, Store} = cbt_btree:open(State, Fd, Options1),

                        %% replace the store with the new value
                        Stores2 = cowdb_util:set_property(StoreId, Store,
                                                          Stores),
                        Db2 = Db#db{stores=Stores2},
                        {ok, Db2}
                end

        end);
open(_, _, _) ->
    {error, bad_transaction_state}.


%% delete a store in the database. It can only happen on a version
%% change transaction.
%%
%% Warning: deleting a store only remove the reference to it in the
%% database. Data will be removed during compaction.
-spec delete(cowdb:db(), cowdb:storeid()) -> {ok, cowdb:db()}.
delete(#db{stores=Stores}=Db, StoreId) ->
    ?IF_TRANS(version_change, fun() ->
                Stores2 = cowdb_util:delete_property(StoreId, Stores),
                {ok, Db#db{stores=Stores2}}
        end).


%% wrap the reduce function so we are able to count elements.
%% TODO: make it native?
wrap_reduce_fun(Options) ->
    case lists:keyfind(reduce, 1, Options) of
        false ->
            ReduceFun = fun (reduce, KVs) ->
                    length(KVs);
                (rereduce, Reds) ->
                    lists:sum(Reds)
            end,
            [{reduce, ReduceFun} | Options];
        {_, ReduceFun0} ->
            ReduceFun = fun(reduce, KVs) ->
                    Count = length(KVs),
                    Result = ReduceFun0(reduce, KVs),
                    {Count, Result};
                (rereduce, Reds) ->
                    Count = lists:sum([Count0 || {Count0, _} <- Reds]),
                    UsrReds = [UsrRedsList || {_, UsrRedsList} <- Reds],
                    Result = ReduceFun0(rereduce, UsrReds),
                    {Count, Result}
            end,
            lists:keyreplace(reduce, 1, Options, {reduce, ReduceFun})
    end.
