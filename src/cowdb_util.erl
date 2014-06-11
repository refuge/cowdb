-module(cowdb_util).

-export([set_property/3, delete_property/2]).
-export([apply/2]).

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


