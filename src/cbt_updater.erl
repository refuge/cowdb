-module(cbt_updater).
-behaviour(gen_server).

-export([start_link/5]).
-export([get_state/1]).
-export([stop/1]).


-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).


-include("cbt.hrl").

get_state(Pid) ->
    gen_server:call(Pid, get_db, infinity).

stop(Pid) ->
    try
        gen_server:call(Pid, stop, infinity)
    catch
        exit:{noproc, _} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok
    end.



start_link(DbPid, Fd, Dir, BtreeSpecs, Options) ->
    gen_server:start_link(?MODULE, [DbPid, Fd, Dir, BtreeSpecs, Options], []).

init([DbPid, Fd, Dir, BtreeSpecs, Options]) ->
    CompactDir = cbt_util:get_opt(compact_dir, Options,
                                  filename:dirname(Dir)),
    FileName = filename:basename(Dir),
    {ok, Header} = case lists:member(create, Options) of
        true ->
            Header1 = #db_header{},
            ok = cbt_file:write_header(Fd, Header1),
            cbt_file:delete(CompactDir, FileName ++
                            ".compact"),
            {ok, Header1};
        false ->
            case cbt_file:read_header(Fd) of
                {ok, Header1} -> {ok, Header1};
                no_valid_header ->
                    Header1 = #db_header{},
                    ok = cbt_file:write_header(Fd, Header1),
                    cbt_file:delete(CompactDir, FileName ++
                                    ".compact"),
                    {ok, Header1}
            end
    end,
    Db = init_db(DbPid, Fd, Header, Dir, BtreeSpecs, Options),
    {ok, Db}.


handle_call(get_db, _From, Db) ->
    {reply, Db, Db};

handle_call(stop, _From, Db) ->
    {stop, normal, ok, Db}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _Db) ->
    ok.

init_db(DbPid, Fd, Header, Dir, BtreeSpecs, Options) ->
    case element(2, Header) of
        ?LATEST_DISK_VERSION ->
            DefaultFSyncOptions = [before_header, after_header, on_file_open],
            FSyncOptions = cbt_util:get_opt(fsync_options, Options,
                                            DefaultFSyncOptions),

            case lists:member(on_file_open, FSyncOptions) of
                true ->
                    ok = cbt_file:sync(Fd);
                _ ->
                    ok
            end,

            %% initialise btrees
            Btrees = case Header#db_header.btrees of
                [] ->
                    lists:foldr(fun({Name, Opts}, Acc) ->
                                {ok, Btree} = cbt_btree:open(nil, Opts),
                                [{Name, Btree} | Acc]
                        end, BtreeSpecs, []);
                States ->
                    Btrees1 = lists:foldl(fun({Name, St}, Acc) ->
                                    Opts = cbt_util:get_value(Name, BtreeSpecs,
                                                              []),
                                    {ok, Btree} = cbt_btree:open(St, Opts),
                                    [{Name, Btree} |Acc]
                            end, States, []),
                    lists:reverse(Btrees1)
            end,

            #db{db_pid = DbPid,
                updater_pid = self(),
                fd = Fd,
                btrees = Btrees,
                header = Header,
                dir = Dir,
                options = Options,
                fsync_options = FSyncOptions};
        _ ->
            {error, database_disk_version_error}
    end.


