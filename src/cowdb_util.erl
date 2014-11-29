%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
-module(cowdb_util).

-include("cowdb.hrl").


-define(DEFAULT_COMPACT_LIMIT, 2048000).


-export([set_property/3,
         delete_property/2,
         apply/2,
         timestamp/0,
         get_value/2, get_value/3,
         get_opt/2, get_opt/3,
         reorder_results/2,
         uniqid/0,
         shutdown_sync/1,
         delete_file/1, delete_file/2,
         debug_mode/0,
         set_verbose/1]).

set_property(Key, Value, Props) ->
    case lists:keyfind(Key, 1, Props) of
        false -> [{Key, Value} | Props];
        _ -> lists:keyreplace(Key, 1, Props, {Key, Value})
    end.

delete_property(Key, Props) ->
    case lists:keyfind(Key, 1, Props) of
        false -> Props;
        _ -> lists:keydelete(Key, 1, Props)
    end.

apply(Func, Args) ->
    case Func of
        {M, F, A} ->
            Args1 = Args ++ A,
            erlang:apply(M, F, Args1);
        {M, F} ->
            erlang:apply(M, F, Args);
        F ->
            erlang:apply(F, Args)
    end.

timestamp() ->
    {A, B, _} = os:timestamp(),
    (A * 1000000) + B.

get_value(Key, List) ->
    get_value(Key, List, undefined).

get_value(Key, List, Default) ->
    case lists:keysearch(Key, 1, List) of
    {value, {Key,Value}} ->
        Value;
    false ->
        Default
    end.

get_opt(Key, Opts) ->
    get_opt(Key, Opts, undefined).

get_opt(Key, Opts, Default) ->
    case proplists:get_value(Key, Opts) of
        undefined ->
            case application:get_env(?MODULE, Key) of
                {ok, Value} -> Value;
                undefined -> Default
            end;
        Value ->
            Value
    end.

% linear search is faster for small lists, length() is 0.5 ms for 100k list
reorder_results(Keys, SortedResults) when length(Keys) < 100 ->
    [get_value(Key, SortedResults) || Key <- Keys];
reorder_results(Keys, SortedResults) ->
    KeyDict = dict:from_list(SortedResults),
    [dict:fetch(Key, KeyDict) || Key <- Keys].

uniqid() ->
    integer_to_list(erlang:phash2(make_ref())).


%% @doc synchronous shutdown
shutdown_sync(Pid) when not is_pid(Pid)->
    ok;
shutdown_sync(Pid) ->
    MRef = erlang:monitor(process, Pid),
    try
        catch unlink(Pid),
        catch exit(Pid, shutdown),
        receive
        {'DOWN', MRef, _, _, _} ->
            receive
            {'EXIT', Pid, _} ->
                ok
            after 0 ->
                ok
            end
        end
    after
        erlang:demonitor(MRef, [flush])
    end.

%% @doc delete a file safely
delete_file(FilePath) ->
    delete_file(FilePath, false).

delete_file(FilePath, Async) ->
    DelFile = FilePath ++ uniqid(),
    case file:rename(FilePath, DelFile) of
    ok ->
        if (Async) ->
            spawn(file, delete, [DelFile]),
            ok;
        true ->
            file:delete(DelFile)
        end;
    Error ->
        Error
    end.

debug_mode() ->
    os:getenv("COWDB_DEBUG") =:= "true".


set_verbose(true) ->
    put(verbose, yes);
set_verbose(_) ->
    erase(verbose).
