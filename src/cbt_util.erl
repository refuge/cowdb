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

-module(cbt_util).

-export([should_flush/0, should_flush/1]).
-export([rand32/0, implode/2]).
-export([encodeBase64Url/1, decodeBase64Url/1]).
-export([get_value/2, get_value/3]).
-export([md5/1, md5_init/0, md5_update/2, md5_final/1]).
-export([reorder_results/2]).
-export([ensure_all_started/1, ensure_all_started/2]).
-export([uniqid/0]).

-ifdef(crypto_compat).
-define(MD5(Data), crypto:md5(Data)).
-define(MD5_INIT(), crypto:md5_init()).
-define(MD5_UPDATE(Ctx, Data), crypto:md5_update(Ctx, Data)).
-define(MD5_FINAL(Ctx), crypto:md5_final(Ctx)).
-else.
-define(MD5(Data), crypto:hash(md5, Data)).
-define(MD5_INIT(), crypto:hash_init(md5)).
-define(MD5_UPDATE(Ctx, Data), crypto:hash_update(Ctx, Data)).
-define(MD5_FINAL(Ctx), crypto:hash_final(Ctx)).
-endif.

-include("cbt.hrl").

% arbitrarily chosen amount of memory to use before flushing to disk
-define(FLUSH_MAX_MEM, 10000000).

-spec ensure_all_started(Application) -> {'ok', Started} | {'error', Reason} when
      Application :: atom(),
      Started :: [atom()],
      Reason :: term().
ensure_all_started(Application) ->
    ensure_all_started(Application, temporary).

-spec ensure_all_started(Application, Type) -> {'ok', Started} | {'error', Reason} when
      Application :: atom(),
      Type ::  'permanent' | 'transient' | 'temporary',
      Started :: [atom()],
      Reason :: term().
ensure_all_started(Application, Type) ->
    case ensure_all_started(Application, Type, []) of
	{ok, Started} ->
	    {ok, lists:reverse(Started)};
	{error, Reason, Started} ->
	    [application:stop(App) || App <- Started],
	    {error, Reason}
    end.

ensure_all_started(Application, Type, Started) ->
    case application:start(Application, Type) of
	ok ->
	    {ok, [Application | Started]};
	{error, {already_started, Application}} ->
	    {ok, Started};
	{error, {not_started, Dependency}} ->
	    case ensure_all_started(Dependency, Type, Started) of
		{ok, NewStarted} ->
		    ensure_all_started(Application, Type, NewStarted);
		Error ->
		    Error
	    end;
	{error, Reason} ->
	    {error, {Application, Reason}, Started}
    end.


get_value(Key, List) ->
    get_value(Key, List, undefined).

get_value(Key, List, Default) ->
    case lists:keysearch(Key, 1, List) of
    {value, {Key,Value}} ->
        Value;
    false ->
        Default
    end.

% returns a random integer
rand32() ->
    crypto:rand_uniform(0, 16#100000000).

implode(List, Sep) ->
    implode(List, Sep, []).

implode([], _Sep, Acc) ->
    lists:flatten(lists:reverse(Acc));
implode([H], Sep, Acc) ->
    implode([], Sep, [H|Acc]);
implode([H|T], Sep, Acc) ->
    implode(T, Sep, [Sep,H|Acc]).

should_flush() ->
    should_flush(?FLUSH_MAX_MEM).

should_flush(MemThreshHold) ->
    {memory, ProcMem} = process_info(self(), memory),
    BinMem = lists:foldl(fun({_Id, Size, _NRefs}, Acc) -> Size+Acc end,
        0, element(2,process_info(self(), binary))),
    if ProcMem+BinMem > 2*MemThreshHold ->
        garbage_collect(),
        {memory, ProcMem2} = process_info(self(), memory),
        BinMem2 = lists:foldl(fun({_Id, Size, _NRefs}, Acc) -> Size+Acc end,
            0, element(2,process_info(self(), binary))),
        ProcMem2+BinMem2 > MemThreshHold;
    true -> false end.

encodeBase64Url(Url) ->
    Url1 = re:replace(base64:encode(Url), ["=+", $$], ""),
    Url2 = re:replace(Url1, "/", "_", [global]),
    re:replace(Url2, "\\+", "-", [global, {return, binary}]).

decodeBase64Url(Url64) ->
    Url1 = re:replace(Url64, "-", "+", [global]),
    Url2 = re:replace(Url1, "_", "/", [global]),
    Padding = lists:duplicate((4 - iolist_size(Url2) rem 4) rem 4, $=),
    base64:decode(iolist_to_binary([Url2, Padding])).

-spec md5(Data::(iolist() | binary())) -> Digest::binary().
md5(Data) ->
    ?MD5(Data).

-spec md5_init() -> Context::binary().
md5_init() ->
    ?MD5_INIT().

-spec md5_update(Context::binary(), Data::(iolist() | binary())) ->
    NewContext::binary().
md5_update(Ctx, D) ->
   ?MD5_UPDATE(Ctx, D).

-spec md5_final(Context::binary()) -> Digest::binary().
md5_final(Ctx) ->
    ?MD5_FINAL(Ctx).

% linear search is faster for small lists, length() is 0.5 ms for 100k list
reorder_results(Keys, SortedResults) when length(Keys) < 100 ->
    [cbt_util:get_value(Key, SortedResults) || Key <- Keys];
reorder_results(Keys, SortedResults) ->
    KeyDict = dict:from_list(SortedResults),
    [dict:fetch(Key, KeyDict) || Key <- Keys].

uniqid() ->
    integer_to_list(erlang:phash2(make_ref())).
