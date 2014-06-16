-module(cowdb_util).

-include("cowdb.hrl").
-include_lib("cbt/include/cbt.hrl").


-define(DEFAULT_COMPACT_LIMIT, 2048000).


-export([set_property/3,
         delete_property/2,
         apply/2,
         timestamp/0,
         init_db/6,
         maybe_sync/3,
         write_header/2,
         commit_transaction/2,
         shutdown_sync/1,
         delete_file/1, delete_file/2]).

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

    CompactLimit = cbt_util:get_opt(compact_limit, Options,
                                    ?DEFAULT_COMPACT_LIMIT),
    AutoCompact = cbt_util:get_opt(auto_compact, Options, true),

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
    {UsrReduce, Reduce} = by_id_reduce(Options),

    {ok, IdBt} = cbt_btree:open(IdP, Fd, [{compression, Compression},
                                          {less, Less},
                                          {reduce, Reduce}]),

    {ok, LogBt} = cbt_btree:open(LogP, Fd, [{compression, Compression},
                                            {reduce, fun log_reduce/2}]),

    %% initial db record
    #db{tid=Tid,
        start_time=timestamp(),
        db_pid=DbPid,
        updater_pid=self(),
        fd=Fd,
        reader_fd=ReaderFd,
        by_id=IdBt,
        log=LogBt,
        header=Header,
        file_path=FilePath,
        fsync_options=FSyncOptions,
        auto_compact=AutoCompact,
        compact_limit=CompactLimit,
        reduce_fun=UsrReduce,
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
    DelFile = FilePath ++ cbt_util:uniqid(),
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

%% private functions
%%
by_id_reduce(Options) ->
    case lists:keyfind(reduce, 1, Options) of
        false ->
            {nil, fun (reduce, KVs) ->
                    Count = length(KVs),
                    Size = lists:sum([S || {_, {_, {_, S}, _, _}} <- KVs]),
                    {Count, Size};
                (rereduce, Reds) ->
                    Count = lists:sum([Count0 || {Count0, _} <- Reds]),
                    AccSize = lists:sum([Size || {_, Size} <- Reds]),
                    {Count, AccSize}
            end};
        {_, ReduceFun0} ->
            {ReduceFun0, fun(reduce, KVs) ->
                    Count = length(KVs),
                    Size = lists:sum([S || {_, {_, {_, S}, _, _}} <- KVs]),
                    Result = ReduceFun0(reduce, KVs),
                    {Count, Size, Result};
                (rereduce, Reds) ->
                    Count = lists:sum([Count0 || {Count0, _, _} <- Reds]),
                    AccSize = lists:sum([Size || {_, Size, _} <- Reds]),
                    UsrReds = [UsrRedsList || {_, UsrRedsList} <- Reds],
                    Result = ReduceFun0(rereduce, UsrReds),
                    {Count, AccSize, Result}
            end}
    end.


log_reduce(reduce, KVS) ->
    length(KVS);
log_reduce(rereduce, Reds) ->
    lists:sum(Reds).
