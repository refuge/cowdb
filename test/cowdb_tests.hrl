%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%

-include_lib("eunit/include/eunit.hrl").

-define(BUILDDIR, filename:absname(
        filename:join([
                filename:dirname(code:which(?MODULE)),
                ".."]))).

-define(TEMPDIR,
    filename:join([?BUILDDIR, "test", "temp"])).

-define(tempfile,
    fun() ->
        {A, B, C} = erlang:now(),
        N = node(),
        FileName = lists:flatten(io_lib:format("~p-~p.~p.~p", [N, A, B, C])),
        filename:join([?TEMPDIR, FileName])
    end).

-define(tempdb,
    fun() ->
            Nums = tuple_to_list(erlang:now()),
            Prefix = "eunit-test-db",
            Suffix = lists:concat([integer_to_list(Num) || Num <- Nums]),
            list_to_binary(Prefix ++ "-" ++ Suffix)
    end).
