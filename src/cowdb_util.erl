-module(cowdb_util).

-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").


-export([set_property/3,
         delete_property/2,
         apply/2,
         timestamp/0,
         init_db/6,
         maybe_sync/3,
         write_header/2,
         commit_transaction/2]).

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


%% @doc initialise the db
init_db(Header, DbPid, Fd, ReaderFd, FilePath, Options) ->
    DefaultFSyncOptions = [before_header, after_header, on_file_open],
    FSyncOptions = cbt_util:get_opt(fsync_options, Options,
                                    DefaultFSyncOptions),

    %% maybe sync the header
    ok = maybe_sync(on_file_open, Fd, FSyncOptions),

    %% extract infos from the header
    #db_header{tid=Tid,
               by_id=IdP,
               log=LogP} = Header,


    %% initialise the btrees
    Compression = cbt_util:get_opt(compression, Options, ?DEFAULT_COMPRESSION),
    DefaultLess = fun(A, B) -> A < B end,
    Less = cbt_util:get_opt(less, Options, DefaultLess),
    Reduce = by_id_reduce(Options),

    {ok, IdBt} = cbt_btree:open(IdP, Fd, [{compression, Compression},
                                          {less, Less},
                                          {reduce, Reduce}]),

    {ok, LogBt} = cbt_btree:open(LogP, Fd, [{compression, Compression},
                                            {reduce, fun log_reduce/2}]),

    %% initial db record
    #db{tid=Tid,
        db_pid=DbPid,
        updater_pid=self(),
        fd=Fd,
        reader_fd=ReaderFd,
        by_id=IdBt,
        log=LogBt,
        header=Header,
        file_path=FilePath,
        fsync_options=FSyncOptions,
        options=Options}.

%% @doc test if the db file should be synchronized or not depending on the
%% state
maybe_sync(Status, Fd, FSyncOptions) ->
    case lists:member(Status, FSyncOptions) of
        true ->
            ok = cbt_file:sync(Fd),
            ok;
        _ ->
            ok
    end.


%% @doc write the db header
write_header(Header, #db{fd=Fd, fsync_options=FsyncOptions}) ->
    ok = maybe_sync(before_header, Fd, FsyncOptions),
    {ok, _} = cbt_file:write_header(Fd, Header),
    ok = maybe_sync(after_headerr, Fd, FsyncOptions),
    ok.


%% @doc commit the transction on the disk.
commit_transaction(TransactId, #db{by_id=IdBt,
                                   log=LogBt,
                                   header=OldHeader}=Db) ->

    %% write the header
    NewHeader = OldHeader#db_header{tid=TransactId,
                                    by_id=cbt_btree:get_state(IdBt),
                                    log=cbt_btree:get_state(LogBt)},
    ok = cowdb_util:write_header(NewHeader, Db),
    {ok, Db#db{tid=TransactId, header=NewHeader}}.


%% private functions
%%
by_id_reduce(Options) ->
    case lists:keyfind(reduce, 1, Options) of
        false ->
            fun (reduce, KVs) ->
                    length(KVs);
                (rereduce, Reds) ->
                    lists:sum(Reds)
            end;
        {_, ReduceFun0} ->
            fun(reduce, KVs) ->
                    Count = length(KVs),
                    Result = ReduceFun0(reduce, KVs),
                    {Count, Result};
                (rereduce, Reds) ->
                    Count = lists:sum([Count0 || {Count0, _} <- Reds]),
                    UsrReds = [UsrRedsList || {_, UsrRedsList} <- Reds],
                    Result = ReduceFun0(rereduce, UsrReds),
                    {Count, Result}
            end
    end.


log_reduce(reduce, KVS) ->
    length(KVS);
log_reduce(rereduce, Reds) ->
    lists:sum(Reds).
