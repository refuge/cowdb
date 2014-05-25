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

-module(cbt_btree).

-export([new/1]).
-export([open/2, open/3]).
-export([query_modify/4, add/2, add_remove/3]).
-export([lookup/2]).
-export([fold/3, fold/4]).
-export([fold_reduce/4, full_reduce/1, final_reduce/2]).
-export([size/1]).
-export([get_state/1]).
-export([set_options/2]).
-export([less/3]).

-include("cbt.hrl").

-define(CHUNK_THRESHOLD, 16#4ff).

-type cbtree() :: #btree{}.
-type cbtree_options() :: [{split, fun()} | {join, fun()} | {less, fun()}
                        | {reduce, fun()}
                        | {compression, cbt_compress:compression_method()}
                        | {chunk_threshold, integer()}].

-type cbt_kv() :: {Key::any(), Val::any()}.
-type cbt_kvs() :: [cbt_kv()].
-type cbt_keys() :: [term()].

-type cbt_fold_options() :: [{dir, fwd | rev} | {start_key, term()} |
                             {end_key, term()} | {end_key_gt, term()} |
                             {key_group_fun, fun()}].

-export_type([cbtree/0]).
-export_type([cbtree_options/0]).
-export_type([cbt_kv/0, cbt_kvs/0]).
-export_type([cbt_keys/0]).
-export_type([cbt_fold_options/0]).

%% @doc create a new btree
-spec new(Fd::cbt_file:cbt_file()) -> {ok, cbtree()}.
new(Fd) ->
    open(nil, Fd).

%% @doc open a btree from the file.
% pass in 'nil' for State if a new Btree.
-spec open(State::nil | cbtree(), Fd::cbt_file:cbt_file()) -> {ok, cbtree()}.
open(State, Fd) ->
    {ok, #btree{root=State, fd=Fd}}.


%% @doc open a btree from the file.
%% pass in 'nil' for State if a new Btree.
%% Options:
%% <ul>
%% <li> {split, fun(Btree, Value)} : Take a value and extract content if
%% needed from it. It returns a {key, Value} tuple. You don't need to
%% set such function if you already give a {Key, Value} tuple to your
%% add/add_remove functions.</li>
%% <li>{join, fun(Key, Value)} : The fonction takes the key and value and
%% return a new Value ussed when you lookup. By default it return a
%% {Key, Value} .</li>
%% <li>{reduce_fun, ReduceFun} : pass the reduce fun</li>
%% <li> {compression, nonde | snappy}: the compression methods used to
%% compress the data</li>
%% <li>{less, LessFun(KeyA, KeyB)}: function used to order the btree that
%% compare two keys</li>
%% </ul>
-spec open(State::nil | cbtree(), Fd::cbt_file:cbt_file(),
           Options::cbtree_options()) -> {ok, cbtree()}.
open(State, Fd, Options) ->
    {ok, set_options(#btree{root=State, fd=Fd}, Options)}.


%% @doc return the latest btree root that will be stored in the database
%% header or value
-spec get_state(Btree::cbtree()) -> State::tuple().
get_state(#btree{root=Root}) ->
    Root.

%% @doc set btreee options
-spec set_options(Btree::cbtree(), Options::cbtree_options()) -> Btree2::cbtree().
set_options(Bt, []) ->
    Bt;
set_options(Bt, [{split, Extract}|Rest]) ->
    set_options(Bt#btree{extract_kv=Extract}, Rest);
set_options(Bt, [{join, Assemble}|Rest]) ->
    set_options(Bt#btree{assemble_kv=Assemble}, Rest);
set_options(Bt, [{less, Less}|Rest]) ->
    set_options(Bt#btree{less=Less}, Rest);
set_options(Bt, [{reduce, Reduce}|Rest]) ->
    set_options(Bt#btree{reduce=Reduce}, Rest);
set_options(Bt, [{compression, Comp}|Rest]) ->
    set_options(Bt#btree{compression=Comp}, Rest);
set_options(Bt, [{chunk_threshold, Threshold}|Rest]) ->
    set_options(Bt#btree{chunk_threshold = Threshold}, Rest).


%% @doc return the size in bytes of a btree
-spec size(Btree::cbtree()) -> Size::integer().
size(#btree{root = nil}) ->
    0;
size(#btree{root = {_P, _Red, Size}}) ->
    Size.


%% --------------------------------
%% Btree  updates methods
%% --------------------------------

%% @doc insert a list of key/values in the btree
-spec add(Btree::cbtree(), InsertKeyValues::cbt_kvs()) ->
    {ok, Btree2::cbtree()}.
add(Bt, InsertKeyValues) ->
    add_remove(Bt, InsertKeyValues, []).

%% @doc insert and remove a list of key/values in the btree in one
%% write.
-spec add_remove(Btree::cbtree(), InsertKeyValues::cbt_kvs(),
          RemoveKeys::cbt_keys()) -> {ok, Btree2::cbtree()}.
add_remove(Bt, InsertKeyValues, RemoveKeys) ->
    {ok, [], Bt2} = query_modify(Bt, [], InsertKeyValues, RemoveKeys),
    {ok, Bt2}.

%% @doc insert and remove a list of key/values and retrieve a list of
%% key/values from their key in the btree in one call.
-spec query_modify(Btree::cbtree(), LookupKeys::cbt_keys(),
                   InsertKeyValues::cbt_kvs(), RemoveKeys::cbt_keys()) ->
    {ok, FoundKeyValues::cbt_kvs(), Btree2::cbtree()}.
query_modify(Bt, LookupKeys, InsertValues, RemoveKeys) ->
    #btree{root=Root} = Bt,
    InsertActions = lists:map(
        fun(KeyValue) ->
            {Key, Value} = extract(Bt, KeyValue),
            {insert, Key, Value}
        end, InsertValues),
    RemoveActions = [{remove, Key, nil} || Key <- RemoveKeys],
    FetchActions = [{fetch, Key, nil} || Key <- LookupKeys],
    SortFun =
        fun({OpA, A, _}, {OpB, B, _}) ->
            case A == B of
            % A and B are equal, sort by op.
            true -> op_order(OpA) < op_order(OpB);
            false ->
                less(Bt, A, B)
            end
        end,
    Actions = lists:sort(SortFun, lists:append([InsertActions, RemoveActions, FetchActions])),
    {ok, KeyPointers, QueryResults} = modify_node(Bt, Root, Actions, []),
    {ok, NewRoot} = complete_root(Bt, KeyPointers),
    {ok, QueryResults, Bt#btree{root=NewRoot}}.


%% --------------------------------
%% Btree query methods
%% --------------------------------

%% @doc lookup for a list of keys in the btree
%% Results are returned in the same order as the keys. If the key is
%% not_found the `not_found' result is appended to the list.
-spec lookup(Btree::cbtree(), Keys::cbt_keys()) -> [{ok, cbt_kv()} | not_found].
lookup(#btree{root=Root, less=Less}=Bt, Keys) ->
    SortedKeys = case Less of
        undefined -> lists:sort(Keys);
        _ -> lists:sort(Less, Keys)
    end,
    {ok, SortedResults} = lookup(Bt, Root, SortedKeys),
    % We want to return the results in the same order as the keys were input
    % but we may have changed the order when we sorted. So we need to put the
    % order back into the results.
    cbt_util:reorder_results(Keys, SortedResults).

lookup(_Bt, nil, Keys) ->
    {ok, [{Key, not_found} || Key <- Keys]};
lookup(Bt, Node, Keys) ->
    Pointer = element(1, Node),
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        lookup_kpnode(Bt, list_to_tuple(NodeList), 1, Keys, []);
    kv_node ->
        lookup_kvnode(Bt, list_to_tuple(NodeList), 1, Keys, [])
    end.

%% @doc fold key/values in the btree
-spec fold(Btree::cbtree(), Fun::fun(), Acc::term()) ->
    {ok, {KVs::cbt_kvs(), Reductions::[term()]}, Acc2::term()}.
fold(Bt, Fun, Acc) ->
    fold(Bt, Fun, Acc, []).

-spec fold(Btree::cbtree(), Fun::fun(), Acc::term(),
           Options::cbt_fold_options()) ->
    {ok, {KVs::cbt_kvs(), Reductions::[term()]}, Acc2::term()}.
fold(#btree{root=nil}, _Fun, Acc, _Options) ->
    {ok, {[], []}, Acc};
fold(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = cbt_util:get_value(dir, Options, fwd),
    InRange = make_key_in_end_range_function(Bt, Dir, Options),
    Result =
    case cbt_util:get_value(start_key, Options) of
    undefined ->
        stream_node(Bt, [], Bt#btree.root, InRange, Dir,
                convert_fun_arity(Fun), Acc);
    StartKey ->
        stream_node(Bt, [], Bt#btree.root, StartKey, InRange, Dir,
                convert_fun_arity(Fun), Acc)
    end,
    case Result of
    {ok, Acc2}->
        FullReduction = element(2, Root),
        {ok, {[], [FullReduction]}, Acc2};
    {stop, LastReduction, Acc2} ->
        {ok, LastReduction, Acc2}
    end.


%% @doc apply the reduce function on last reductions.
-spec final_reduce(Btree::cbtree(), LastReduction::{any(), any()}) -> term().
final_reduce(#btree{reduce=Reduce}, Val) ->
    do_final_reduce(Reduce, Val).

do_final_reduce(Reduce, {[], []}) ->
    Reduce(reduce, []);
do_final_reduce(_Bt, {[], [Red]}) ->
    Red;
do_final_reduce(Reduce, {[], Reductions}) ->
    Reduce(rereduce, Reductions);
do_final_reduce(Reduce, {KVs, Reductions}) ->
    Red = Reduce(reduce, KVs),
    do_final_reduce(Reduce, {[], [Red | Reductions]}).

%% @doc fold reduce values.
%%
-spec fold_reduce(Btree::cbtree(), FoldFun::fun(), Acc::any(),
                    Options::cbt_fold_options()) -> {ok, Acc2::term()}.
fold_reduce(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = cbt_util:get_value(dir, Options, fwd),
    StartKey = cbt_util:get_value(start_key, Options),
    InEndRangeFun = make_key_in_end_range_function(Bt, Dir, Options),
    KeyGroupFun = cbt_util:get_value(key_group_fun, Options, fun(_,_) -> true end),
    try
        {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, Root, StartKey, InEndRangeFun, undefined, [], [],
            KeyGroupFun, Fun, Acc),
        if GroupedKey2 == undefined ->
            {ok, Acc2};
        true ->
            case Fun(GroupedKey2, {GroupedKVsAcc2, GroupedRedsAcc2}, Acc2) of
            {ok, Acc3} -> {ok, Acc3};
            {stop, Acc3} -> {ok, Acc3}
            end
        end
    catch
        throw:{stop, AccDone} -> {ok, AccDone}
    end.

%% @doc return the full reduceed value from the btree.
-spec full_reduce(Btree::cbtree()) -> {ok, term()}.
full_reduce(#btree{root=nil,reduce=Reduce}) ->
    {ok, Reduce(reduce, [])};
full_reduce(#btree{root=Root}) ->
    {ok, element(2, Root)}.



extract(#btree{extract_kv=undefined}, Value) ->
    Value;
extract(#btree{extract_kv=Extract}, Value) ->
    Extract(Value).

assemble(#btree{assemble_kv=undefined}, Key, Value) ->
    {Key, Value};
assemble(#btree{assemble_kv=Assemble}, Key, Value) ->
    Assemble(Key, Value).


less(#btree{less=undefined}, A, B) ->
    A < B;
less(#btree{less=Less}, A, B) ->
    Less(A, B).

%% ------------------------------------
%% PRIVATE API
%% ------------------------------------

% wraps a 2 arity function with the proper 3 arity function
convert_fun_arity(Fun) when is_function(Fun, 2) ->
    fun
        (visit, KV, _Reds, AccIn) -> Fun(KV, AccIn);
        (traverse, _K, _Red, AccIn) -> {ok, AccIn}
    end;
convert_fun_arity(Fun) when is_function(Fun, 3) ->
    fun
        (visit, KV, Reds, AccIn) -> Fun(KV, Reds, AccIn);
        (traverse, _K, _Red, AccIn) -> {ok, AccIn}
    end;
convert_fun_arity(Fun) when is_function(Fun, 4) ->
    Fun.    % Already arity 4

make_key_in_end_range_function(Bt, fwd, Options) ->
    case cbt_util:get_value(end_key_gt, Options) of
    undefined ->
        case cbt_util:get_value(end_key, Options) of
        undefined ->
            fun(_Key) -> true end;
        LastKey ->
            fun(Key) -> not less(Bt, LastKey, Key) end
        end;
    EndKey ->
        fun(Key) -> less(Bt, Key, EndKey) end
    end;
make_key_in_end_range_function(Bt, rev, Options) ->
    case cbt_util:get_value(end_key_gt, Options) of
    undefined ->
        case cbt_util:get_value(end_key, Options) of
        undefined ->
            fun(_Key) -> true end;
        LastKey ->
            fun(Key) -> not less(Bt, Key, LastKey) end
        end;
    EndKey ->
        fun(Key) -> less(Bt, EndKey, Key) end
    end.




% for ordering different operations with the same key.
% fetch < remove < insert
op_order(fetch) -> 1;
op_order(remove) -> 2;
op_order(insert) -> 3.


lookup_kpnode(_Bt, _NodeTuple, _LowerBound, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kpnode(_Bt, NodeTuple, LowerBound, Keys, Output) when tuple_size(NodeTuple) < LowerBound ->
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kpnode(Bt, NodeTuple, LowerBound, [FirstLookupKey | _] = LookupKeys, Output) ->
    N = find_first_gteq(Bt, NodeTuple, LowerBound, tuple_size(NodeTuple), FirstLookupKey),
    {Key, PointerInfo} = element(N, NodeTuple),
    SplitFun = fun(LookupKey) -> not less(Bt, Key, LookupKey) end,
    case lists:splitwith(SplitFun, LookupKeys) of
    {[], GreaterQueries} ->
        lookup_kpnode(Bt, NodeTuple, N + 1, GreaterQueries, Output);
    {LessEqQueries, GreaterQueries} ->
        {ok, Results} = lookup(Bt, PointerInfo, LessEqQueries),
        lookup_kpnode(Bt, NodeTuple, N + 1, GreaterQueries, lists:reverse(Results, Output))
    end.


lookup_kvnode(_Bt, _NodeTuple, _LowerBound, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kvnode(_Bt, NodeTuple, LowerBound, Keys, Output) when tuple_size(NodeTuple) < LowerBound ->
    % keys not found
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kvnode(Bt, NodeTuple, LowerBound, [LookupKey | RestLookupKeys], Output) ->
    N = find_first_gteq(Bt, NodeTuple, LowerBound, tuple_size(NodeTuple), LookupKey),
    {Key, Value} = element(N, NodeTuple),
    case less(Bt, LookupKey, Key) of
    true ->
        % LookupKey is less than Key
        lookup_kvnode(Bt, NodeTuple, N, RestLookupKeys, [{LookupKey, not_found} | Output]);
    false ->
        case less(Bt, Key, LookupKey) of
        true ->
            % LookupKey is greater than Key
            lookup_kvnode(Bt, NodeTuple, N+1, RestLookupKeys, [{LookupKey, not_found} | Output]);
        false ->
            % LookupKey is equal to Key
            lookup_kvnode(Bt, NodeTuple, N, RestLookupKeys, [{LookupKey, {ok, assemble(Bt, LookupKey, Value)}} | Output])
        end
    end.


complete_root(_Bt, []) ->
    {ok, nil};
complete_root(_Bt, [{_Key, PointerInfo}])->
    {ok, PointerInfo};
complete_root(Bt, KPs) ->
    {ok, ResultKeyPointers} = write_node(Bt, kp_node, KPs),
    complete_root(Bt, ResultKeyPointers).

%%%%%%%%%%%%% The chunkify function sucks! %%%%%%%%%%%%%
% It is inaccurate as it does not account for compression when blocks are
% written. Plus with the "case byte_size(term_to_binary(InList)) of" code
% it's probably really inefficient.

chunkify(#btree{chunk_threshold = ChunkThreshold0}, InList) ->
    case ?term_size(InList) of
    Size when Size > ChunkThreshold0 ->
        NumberOfChunksLikely = ((Size div ChunkThreshold0) + 1),
        ChunkThreshold = Size div NumberOfChunksLikely,
        chunkify(InList, ChunkThreshold, [], 0, []);
    _Else ->
        [InList]
    end.

chunkify([], _ChunkThreshold, [], 0, OutputChunks) ->
    lists:reverse(OutputChunks);
chunkify([], _ChunkThreshold, OutList, _OutListSize, OutputChunks) ->
    lists:reverse([lists:reverse(OutList) | OutputChunks]);
chunkify([InElement | RestInList], ChunkThreshold, OutList, OutListSize, OutputChunks) ->
    case ?term_size(InElement) of
    Size when (Size + OutListSize) > ChunkThreshold andalso OutList /= [] ->
        chunkify(RestInList, ChunkThreshold, [], 0, [lists:reverse([InElement | OutList]) | OutputChunks]);
    Size ->
        chunkify(RestInList, ChunkThreshold, [InElement | OutList], OutListSize + Size, OutputChunks)
    end.

modify_node(Bt, RootPointerInfo, Actions, QueryOutput) ->
    {NodeType, NodeList} = case RootPointerInfo of
        nil ->
            {kv_node, []};
        _Tuple ->
            Pointer = element(1, RootPointerInfo),
            get_node(Bt, Pointer)
    end,
    NodeTuple = list_to_tuple(NodeList),

    {ok, NewNodeList, QueryOutput2} =  case NodeType of
        kp_node -> modify_kpnode(Bt, NodeTuple, 1, Actions, [], QueryOutput);
        kv_node -> modify_kvnode(Bt, NodeTuple, 1, Actions, [], QueryOutput)
    end,
    case NewNodeList of
        [] ->  % no nodes remain
            {ok, [], QueryOutput2};
        NodeList ->  % nothing changed
            {LastKey, _LastValue} = element(tuple_size(NodeTuple), NodeTuple),
            {ok, [{LastKey, RootPointerInfo}], QueryOutput2};
        _Else2 ->
            {ok, ResultList} = write_node(Bt, NodeType, NewNodeList),
            {ok, ResultList, QueryOutput2}
    end.

reduce_node(#btree{reduce=nil}, _NodeType, _NodeList) ->
    [];
reduce_node(#btree{reduce=R}, kp_node, NodeList) ->
    R(rereduce, [element(2, Node) || {_K, Node} <- NodeList]);
reduce_node(#btree{reduce=R}=Bt, kv_node, NodeList) ->
    R(reduce, [assemble(Bt, K, V) || {K, V} <- NodeList]).

reduce_tree_size(kv_node, NodeSize, _KvList) ->
    NodeSize;
reduce_tree_size(kp_node, NodeSize, []) ->
    NodeSize;
reduce_tree_size(kp_node, _NodeSize, [{_K, {_P, _Red, nil}} | _]) ->
    nil;
reduce_tree_size(kp_node, NodeSize, [{_K, {_P, _Red, Sz}} | NodeList]) ->
    reduce_tree_size(kp_node, NodeSize + Sz, NodeList).



get_node(#btree{fd = Fd}, NodePos) ->
    {ok, {NodeType, NodeList}} = cbt_file:pread_term(Fd, NodePos),
    {NodeType, NodeList}.

write_node(#btree{fd = Fd, compression = Comp} = Bt, NodeType, NodeList) ->
    % split up nodes into smaller sizes
    NodeListList = chunkify(Bt, NodeList),
    % now write out each chunk and return the KeyPointer pairs for those nodes
    ResultList = [
        begin
            {ok, Pointer, Size} = cbt_file:append_term(
                Fd, {NodeType, ANodeList}, [{compression, Comp}]),
            {LastKey, _} = lists:last(ANodeList),
            SubTreeSize = reduce_tree_size(NodeType, Size, ANodeList),
            {LastKey, {Pointer, reduce_node(Bt, NodeType, ANodeList), SubTreeSize}}
        end
    ||
        ANodeList <- NodeListList
    ],
    {ok, ResultList}.

modify_kpnode(Bt, {}, _LowerBound, Actions, [], QueryOutput) ->
    modify_node(Bt, nil, Actions, QueryOutput);
modify_kpnode(_Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput) ->
    {ok, lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound,
            tuple_size(NodeTuple), [])), QueryOutput};
modify_kpnode(Bt, NodeTuple, LowerBound,
        [{_, FirstActionKey, _}|_]=Actions, ResultNode, QueryOutput) ->
    Sz = tuple_size(NodeTuple),
    N = find_first_gteq(Bt, NodeTuple, LowerBound, Sz, FirstActionKey),
    case N =:= Sz of
    true  ->
        % perform remaining actions on last node
        {_, PointerInfo} = element(Sz, NodeTuple),
        {ok, ChildKPs, QueryOutput2} =
            modify_node(Bt, PointerInfo, Actions, QueryOutput),
        NodeList = lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound,
            Sz - 1, ChildKPs)),
        {ok, NodeList, QueryOutput2};
    false ->
        {NodeKey, PointerInfo} = element(N, NodeTuple),
        SplitFun = fun({_ActionType, ActionKey, _ActionValue}) ->
                not less(Bt, NodeKey, ActionKey)
            end,
        {LessEqQueries, GreaterQueries} = lists:splitwith(SplitFun, Actions),
        {ok, ChildKPs, QueryOutput2} =
                modify_node(Bt, PointerInfo, LessEqQueries, QueryOutput),
        ResultNode2 = lists:reverse(ChildKPs, bounded_tuple_to_revlist(NodeTuple,
                LowerBound, N - 1, ResultNode)),
        modify_kpnode(Bt, NodeTuple, N+1, GreaterQueries, ResultNode2, QueryOutput2)
    end.

bounded_tuple_to_revlist(_Tuple, Start, End, Tail) when Start > End ->
    Tail;
bounded_tuple_to_revlist(Tuple, Start, End, Tail) ->
    bounded_tuple_to_revlist(Tuple, Start+1, End, [element(Start, Tuple)|Tail]).

bounded_tuple_to_list(Tuple, Start, End, Tail) ->
    bounded_tuple_to_list2(Tuple, Start, End, [], Tail).

bounded_tuple_to_list2(_Tuple, Start, End, Acc, Tail) when Start > End ->
    lists:reverse(Acc, Tail);
bounded_tuple_to_list2(Tuple, Start, End, Acc, Tail) ->
    bounded_tuple_to_list2(Tuple, Start + 1, End, [element(Start, Tuple) | Acc], Tail).

find_first_gteq(_Bt, _Tuple, Start, End, _Key) when Start == End ->
    End;
find_first_gteq(Bt, Tuple, Start, End, Key) ->
    Mid = Start + ((End - Start) div 2),
    {TupleKey, _} = element(Mid, Tuple),
    case less(Bt, TupleKey, Key) of
    true ->
        find_first_gteq(Bt, Tuple, Mid+1, End, Key);
    false ->
        find_first_gteq(Bt, Tuple, Start, Mid, Key)
    end.

modify_kvnode(_Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput) ->
    {ok, lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound, tuple_size(NodeTuple), [])), QueryOutput};
modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], ResultNode, QueryOutput) when LowerBound > tuple_size(NodeTuple) ->
    case ActionType of
    insert ->
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
    remove ->
        % just drop the action
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, QueryOutput);
    fetch ->
        % the key/value must not exist in the tree
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput])
    end;
modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], AccNode, QueryOutput) ->
    N = find_first_gteq(Bt, NodeTuple, LowerBound, tuple_size(NodeTuple), ActionKey),
    {Key, Value} = element(N, NodeTuple),
    ResultNode =  bounded_tuple_to_revlist(NodeTuple, LowerBound, N - 1, AccNode),
    case less(Bt, ActionKey, Key) of
    true ->
        case ActionType of
        insert ->
            % ActionKey is less than the Key, so insert
            modify_kvnode(Bt, NodeTuple, N, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
        remove ->
            % ActionKey is less than the Key, just drop the action
            modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, QueryOutput);
        fetch ->
            % ActionKey is less than the Key, the key/value must not exist in the tree
            modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput])
        end;
    false ->
        % ActionKey and Key are maybe equal.
        case less(Bt, Key, ActionKey) of
        false ->
            case ActionType of
            insert ->
                modify_kvnode(Bt, NodeTuple, N+1, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
            remove ->
                modify_kvnode(Bt, NodeTuple, N+1, RestActions, ResultNode, QueryOutput);
            fetch ->
                % ActionKey is equal to the Key, insert into the QueryOuput, but re-process the node
                % since an identical action key can follow it.
                modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{ok, assemble(Bt, Key, Value)} | QueryOutput])
            end;
        true ->
            modify_kvnode(Bt, NodeTuple, N + 1, [{ActionType, ActionKey, ActionValue} | RestActions], [{Key, Value} | ResultNode], QueryOutput)
        end
    end.


reduce_stream_node(_Bt, _Dir, nil, _KeyStart, _InEndRangeFun, GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, _KeyGroupFun, _Fun, Acc) ->
    {ok, Acc, GroupedRedsAcc, GroupedKVsAcc, GroupedKey};
reduce_stream_node(Bt, Dir, Node, KeyStart, InEndRangeFun, GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, KeyGroupFun, Fun, Acc) ->
    P = element(1, Node),
    case get_node(Bt, P) of
    {kp_node, NodeList} ->
        NodeList2 = adjust_dir(Dir, NodeList),
        reduce_stream_kp_node(Bt, Dir, NodeList2, KeyStart, InEndRangeFun, GroupedKey,
                GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc);
    {kv_node, KVs} ->
        KVs2 = adjust_dir(Dir, KVs),
        reduce_stream_kv_node(Bt, Dir, KVs2, KeyStart, InEndRangeFun, GroupedKey,
                GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc)
    end.

reduce_stream_kv_node(Bt, Dir, KVs, KeyStart, InEndRangeFun,
                        GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, Acc) ->

    GTEKeyStartKVs =
    case KeyStart of
    undefined ->
        KVs;
    _ ->
        DropFun = case Dir of
        fwd ->
            fun({Key, _}) -> less(Bt, Key, KeyStart) end;
        rev ->
            fun({Key, _}) -> less(Bt, KeyStart, Key) end
        end,
        lists:dropwhile(DropFun, KVs)
    end,
    KVs2 = lists:takewhile(
        fun({Key, _}) -> InEndRangeFun(Key) end, GTEKeyStartKVs),
    reduce_stream_kv_node2(Bt, KVs2, GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, Acc).


reduce_stream_kv_node2(_Bt, [], GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
        _KeyGroupFun, _Fun, Acc) ->
    {ok, Acc, GroupedRedsAcc, GroupedKVsAcc, GroupedKey};
reduce_stream_kv_node2(Bt, [{Key, Value}| RestKVs], GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, KeyGroupFun, Fun, Acc) ->
    case GroupedKey of
    undefined ->
        reduce_stream_kv_node2(Bt, RestKVs, Key,
                [assemble(Bt,Key,Value)], [], KeyGroupFun, Fun, Acc);
    _ ->

        case KeyGroupFun(GroupedKey, Key) of
        true ->
            reduce_stream_kv_node2(Bt, RestKVs, GroupedKey,
                [assemble(Bt,Key,Value)|GroupedKVsAcc], GroupedRedsAcc, KeyGroupFun,
                Fun, Acc);
        false ->
            case Fun(GroupedKey, {GroupedKVsAcc, GroupedRedsAcc}, Acc) of
            {ok, Acc2} ->
                reduce_stream_kv_node2(Bt, RestKVs, Key, [assemble(Bt,Key,Value)],
                    [], KeyGroupFun, Fun, Acc2);
            {stop, Acc2} ->
                throw({stop, Acc2})
            end
        end
    end.

reduce_stream_kp_node(Bt, Dir, NodeList, KeyStart, InEndRangeFun,
                        GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, Acc) ->
    Nodes =
    case KeyStart of
    undefined ->
        NodeList;
    _ ->
        case Dir of
        fwd ->
            lists:dropwhile(fun({Key, _}) -> less(Bt, Key, KeyStart) end, NodeList);
        rev ->
            RevKPs = lists:reverse(NodeList),
            case lists:splitwith(fun({Key, _}) -> less(Bt, Key, KeyStart) end, RevKPs) of
            {_Before, []} ->
                NodeList;
            {Before, [FirstAfter | _]} ->
                [FirstAfter | lists:reverse(Before)]
            end
        end
    end,
    {InRange, MaybeInRange} = lists:splitwith(
        fun({Key, _}) -> InEndRangeFun(Key) end, Nodes),
    NodesInRange = case MaybeInRange of
    [FirstMaybeInRange | _] when Dir =:= fwd ->
        InRange ++ [FirstMaybeInRange];
    _ ->
        InRange
    end,
    reduce_stream_kp_node2(Bt, Dir, NodesInRange, KeyStart, InEndRangeFun,
        GroupedKey, GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc).


reduce_stream_kp_node2(Bt, Dir, [{_Key, NodeInfo} | RestNodeList], KeyStart, InEndRangeFun,
                        undefined, [], [], KeyGroupFun, Fun, Acc) ->
    {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, NodeInfo, KeyStart, InEndRangeFun, undefined,
                [], [], KeyGroupFun, Fun, Acc),
    reduce_stream_kp_node2(Bt, Dir, RestNodeList, KeyStart, InEndRangeFun, GroupedKey2,
            GroupedKVsAcc2, GroupedRedsAcc2, KeyGroupFun, Fun, Acc2);
reduce_stream_kp_node2(Bt, Dir, NodeList, KeyStart, InEndRangeFun,
        GroupedKey, GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc) ->
    {Grouped0, Ungrouped0} = lists:splitwith(fun({Key,_}) ->
        KeyGroupFun(GroupedKey, Key) end, NodeList),
    {GroupedNodes, UngroupedNodes} =
    case Grouped0 of
    [] ->
        {Grouped0, Ungrouped0};
    _ ->
        [FirstGrouped | RestGrouped] = lists:reverse(Grouped0),
        {RestGrouped, [FirstGrouped | Ungrouped0]}
    end,
    GroupedReds = [element(2, Node) || {_, Node} <- GroupedNodes],
    case UngroupedNodes of
    [{_Key, NodeInfo}|RestNodes] ->
        {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, NodeInfo, KeyStart, InEndRangeFun, GroupedKey,
                GroupedKVsAcc, GroupedReds ++ GroupedRedsAcc, KeyGroupFun, Fun, Acc),
        reduce_stream_kp_node2(Bt, Dir, RestNodes, KeyStart, InEndRangeFun, GroupedKey2,
                GroupedKVsAcc2, GroupedRedsAcc2, KeyGroupFun, Fun, Acc2);
    [] ->
        {ok, Acc, GroupedReds ++ GroupedRedsAcc, GroupedKVsAcc, GroupedKey}
    end.

adjust_dir(fwd, List) ->
    List;
adjust_dir(rev, List) ->
    lists:reverse(List).

stream_node(Bt, Reds, Node, StartKey, InRange, Dir, Fun, Acc) ->
    Pointer = element(1, Node),
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        stream_kp_node(Bt, Reds, adjust_dir(Dir, NodeList), StartKey, InRange, Dir, Fun, Acc);
    kv_node ->
        stream_kv_node(Bt, Reds, adjust_dir(Dir, NodeList), StartKey, InRange, Dir, Fun, Acc)
    end.

stream_node(Bt, Reds, Node, InRange, Dir, Fun, Acc) ->
    Pointer = element(1, Node),
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        stream_kp_node(Bt, Reds, adjust_dir(Dir, NodeList), InRange, Dir, Fun, Acc);
    kv_node ->
        stream_kv_node2(Bt, Reds, [], adjust_dir(Dir, NodeList), InRange, Dir, Fun, Acc)
    end.

stream_kp_node(_Bt, _Reds, [], _InRange, _Dir, _Fun, Acc) ->
    {ok, Acc};
stream_kp_node(Bt, Reds, [{Key, Node} | Rest], InRange, Dir, Fun, Acc) ->
    Red = element(2, Node),
    case Fun(traverse, Key, Red, Acc) of
    {ok, Acc2} ->
        case stream_node(Bt, Reds, Node, InRange, Dir, Fun, Acc2) of
        {ok, Acc3} ->
            stream_kp_node(Bt, [Red | Reds], Rest, InRange, Dir, Fun, Acc3);
        {stop, LastReds, Acc3} ->
            {stop, LastReds, Acc3}
        end;
    {skip, Acc2} ->
        stream_kp_node(Bt, [Red | Reds], Rest, InRange, Dir, Fun, Acc2)
    end.

drop_nodes(_Bt, Reds, _StartKey, []) ->
    {Reds, []};
drop_nodes(Bt, Reds, StartKey, [{NodeKey, Node} | RestKPs]) ->
    case less(Bt, NodeKey, StartKey) of
    true ->
        drop_nodes(Bt, [element(2, Node) | Reds], StartKey, RestKPs);
    false ->
        {Reds, [{NodeKey, Node} | RestKPs]}
    end.

stream_kp_node(Bt, Reds, KPs, StartKey, InRange, Dir, Fun, Acc) ->
    {NewReds, NodesToStream} =
    case Dir of
    fwd ->
        % drop all nodes sorting before the key
        drop_nodes(Bt, Reds, StartKey, KPs);
    rev ->
        % keep all nodes sorting before the key, AND the first node to sort after
        RevKPs = lists:reverse(KPs),
         case lists:splitwith(fun({Key, _Pointer}) -> less(Bt, Key, StartKey) end, RevKPs) of
        {_RevsBefore, []} ->
            % everything sorts before it
            {Reds, KPs};
        {RevBefore, [FirstAfter | Drop]} ->
            {[element(2, Node) || {_K, Node} <- Drop] ++ Reds,
                 [FirstAfter | lists:reverse(RevBefore)]}
        end
    end,
    case NodesToStream of
    [] ->
        {ok, Acc};
    [{_Key, Node} | Rest] ->
        case stream_node(Bt, NewReds, Node, StartKey, InRange, Dir, Fun, Acc) of
        {ok, Acc2} ->
            Red = element(2, Node),
            stream_kp_node(Bt, [Red | NewReds], Rest, InRange, Dir, Fun, Acc2);
        {stop, LastReds, Acc2} ->
            {stop, LastReds, Acc2}
        end
    end.

stream_kv_node(Bt, Reds, KVs, StartKey, InRange, Dir, Fun, Acc) ->
    DropFun =
    case Dir of
    fwd ->
        fun({Key, _}) -> less(Bt, Key, StartKey) end;
    rev ->
        fun({Key, _}) -> less(Bt, StartKey, Key) end
    end,
    {LTKVs, GTEKVs} = lists:splitwith(DropFun, KVs),
    AssembleLTKVs = [assemble(Bt,K,V) || {K,V} <- LTKVs],
    stream_kv_node2(Bt, Reds, AssembleLTKVs, GTEKVs, InRange, Dir, Fun, Acc).

stream_kv_node2(_Bt, _Reds, _PrevKVs, [], _InRange, _Dir, _Fun, Acc) ->
    {ok, Acc};
stream_kv_node2(Bt, Reds, PrevKVs, [{K,V} | RestKVs], InRange, Dir, Fun, Acc) ->
    case InRange(K) of
    false ->
        {stop, {PrevKVs, Reds}, Acc};
    true ->
        AssembledKV = assemble(Bt, K, V),
        case Fun(visit, AssembledKV, {PrevKVs, Reds}, Acc) of
        {ok, Acc2} ->
            stream_kv_node2(Bt, Reds, [AssembledKV | PrevKVs], RestKVs, InRange, Dir, Fun, Acc2);
        {stop, Acc2} ->
            {stop, {PrevKVs, Reds}, Acc2}
        end
    end.
