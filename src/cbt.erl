-module(cbt).
-behaviour(gen_server).

-export([open/2, open/3, open/4,
         open_link/2, open_link/3, open_link/4,
         close/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-export([start_app/0]).

-include("cbt.hrl").

-ifdef(DEBUG).
-define(log(Fmt,Args),io:format(user,Fmt,Args)).
-else.
-define(log(Fmt,Args),ok).
-endif.


%% PUBLIC API
%%
-type cdb() :: pid().

-type config_option() :: {compress, none | gzip | snappy | lz4}
                       | {page_size, pos_integer()}
                       | {read_buffer_size, pos_integer()}
                       | {write_buffer_size, pos_integer()}
                       | {merge_strategy, fast | predictable }
                       | {sync_strategy, none | sync | {seconds, pos_integer()}}
                       | {expiry_secs, non_neg_integer()}
                       | {spawn_opt, list()}.

-type config_options() :: [config_option()].

-type btree_spec() :: {BtreeId::term(), BtreeOptions::list()}.


%% @doc
%% Create or open a cdb store. Argument `Dir' names a
%% directory in which to keep the data files. By convention, we
%% name cdb data directories with extension ".cdb".
- spec open(Dir::string(), Specs::[btree_spec()])
    -> {ok, cdb()} | ignore | {error, term()}.
open(Dir, BtreeSpecs) ->
    open(Dir, BtreeSpecs, []).

%% @doc Create or open a cdb store.
- spec open(Dir::string(), Specs::[btree_spec()], Opts::[config_option()])
    ->  {ok, cdb()} | ignore | {error, term()}.
open(Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt_util:get_opt(spawn_opt, Opts, []),
    gen_server:start(?MODULE, [Dir, BtreeSpecs, Opts], [{spawn_opt,SpawnOpt}]).

%% @doc Create or open a cdb store with a registered name.
- spec open(Name::{local, atom()} | {global, term()} | {via, term()},
            Dir::string(), Specs::[btree_spec()], Opts::config_options())
    -> {ok, cdb()} | ignore | {error, term()}.
open(Name, Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt:get_opt(spawn_opt, Opts, []),
    gen_server:start(Name, ?MODULE, [Dir, BtreeSpecs, Opts], [{spawn_opt,SpawnOpt}]).

%% @doc
%% Create or open a cdb store as part of a supervision tree.
%% Argument `Dir' names a directory in which to keep the data files.
%% By convention, we name cdb data directories with extension
%% ".cdb".
- spec open_link(Dir::string(), Specs::[btree_spec()])
    -> {ok, cdb()} | ignore | {error, term()}.
open_link(Dir, BtreeSpecs) ->
    open_link(Dir, BtreeSpecs, []).

%% @doc Create or open a cdb store as part of a supervision tree.
- spec open_link(Dir::string(), Specs::[btree_spec()],
                 Opts::[config_option()])
    -> {ok, cdb()} | ignore | {error, term()}.
open_link(Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt:get_opt(spawn_opt, Opts, []),
    gen_server:start_link(?MODULE, [Dir, BtreeSpecs, Opts],
                          [{spawn_opt,SpawnOpt}]).

%% @doc Create or open a cdb store as part of a supervision tree
%% with a registered name.
- spec open_link(Name::{local, atom()} | {global, term()} | {via, term()},
                 Dir::string(),  Specs::[btree_spec()],
                 Opts::[config_option()])
    -> {ok, cdb()} | ignore | {error, term()}.
open_link(Name, Dir, BtreeSpecs, Opts) ->
    ok = start_app(),
    SpawnOpt = cbt_util:get_opt(spawn_opt, Opts, []),
    gen_server:start_link(Name, ?MODULE, [Dir, BtreeSpecs, Opts],
                          [{spawn_opt,SpawnOpt}]).

%% @doc
%% Close a cdb data store.
- spec close(Ref::pid()) -> ok.
close(Ref) ->
    try
        gen_server:call(Ref, close, infinity)
    catch
        exit:{noproc, _} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok
    end.


%% gen_server api
%%
%%

init([Dir, BtreeSpecs, Options]) ->
    case cbt_file:open(Dir, Options) of
        {ok, Fd} ->
            {ok, UpdaterPid} = cbt_updater:start_link(self(), Fd, Dir,
                                                      BtreeSpecs,
                                                      Options),
            Db = cbt_updater:get_state(UpdaterPid),
            process_flag(trap_exit, true),
            {ok, Db};
        Error ->
            Error
    end.

handle_call(get_db, _From, Db) ->
    {reply, {ok, Db}, Db};

handle_call({db_updated, Db}, _From, _OldDb) ->
    {reply, ok, Db};

handle_call(close, _From, Db) ->
    {stop, normal, ok, Db}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', _Pid, normal}, Db) ->
    {noreply, Db};

handle_info({'EXIT', Fd, Reason}, #db{fd=Fd}=Db) ->
    error_logger:error_msg("file crashed with reason ~p~n",
                           [Reason]),
    {stop, {error, Reason}, Db};

handle_info({'EXIT', Pid, Reason}, #db{updater_pid=Pid}=Db) ->
    error_logger:error_msg("updater pid crashed with reason ~p~n",
                           [Reason]),

    #db{fd=Fd,
        dir=Dir,
        btree_specs=BtreeSpecs,
        options=Options} = Db,


    case cbt_updater:start_link(self(), Fd, Dir, BtreeSpecs, Options) of
        {ok, UpdaterPid} ->
            NewDb = cbt_updater:get_state(UpdaterPid),
            {noreply, NewDb};
        Error ->
            {stop, Error, Db}
    end;



handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #db{fd=Fd, updater_pid=UpdaterPid}) ->
    ok = cbt_updater:stop(UpdaterPid),
    cbt_file:close(Fd),
    ok.

%% UTILS fonctions
start_app() ->
    {ok, _} = cbt_util:ensure_all_started(cbt),
    ok.
