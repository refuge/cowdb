-module(cowdb_server).
-behaviour(gen_server).

-export([start/0, stop/0]).
-export([all/0, open_db/2, close_db/1, users/1, verbose/1, pid2name/1]).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-include("cowdb.hrl").

%% record for not yet handled reqeusts to open or close files
-record(pending, {db, ref, pid, from, reqtype, clients}). % [{From,Args}]
-record(state, {store, pending}).

-define(REGISTRY, cowdb_registry).  % {Dble, NoUsers, DblePid}
-define(OWNERS, cowdb_owners).      % {DblePid, Dble}
-define(STORE, cowdb).              % {User, Dble} and {{links,User}, NoLinks}



%%-define(DEBUGF(X,Y), io:format(X, Y)).
-define(DEBUGF(X,Y), void).
-compile({inline, [{pid2name_1,1}]}).

start() ->
    ensure_started().

stop() ->
    case whereis(?SERVER_NAME) of
        undefined -> ok;
        _Pid -> application:stop(cowdb)

    end.

all() ->
    call(all).

open_db(Db, OpenArgs) ->
    call({open, Db, OpenArgs}).

close_db(Db) ->
    call({close, Db}).


verbose(What) ->
    call({set_vebose, What}).

users(Db) ->
    call({users, Db}).

pid2name(Pid) ->
    ensure_started(),
    pid2name_1(Pid).


call(Message) ->
    ensure_started(),
    gen_server:call(?SERVER_NAME, Message, infinity).



start_link() ->
    gen_server:start_link({local, ?SERVER_NAME}, ?MODULE, [], []).


%% internal functions

init(_) ->
    set_verbose(verbose_flag()),
    process_flag(trap_exit, true),
    ?REGISTRY = ets:new(?REGISTRY, [set, named_table]),
    ?OWNERS = ets:new(?OWNERS, [set, named_table]),
    Store = ets:new(?STORE, [duplicate_bag]),
    {ok, #state{store=Store, pending=[]}}.

handle_call(all, _From, State) ->
    F = fun(X, A) -> [element(1, X) | A] end,
    {reply, ets:foldl(F, [], ?REGISTRY), State};
handle_call({close, Db}, From, State) ->
    request([{{close, Db}, From}], State);
handle_call({open, Db, OpenArgs}, From, State) ->
    request([{{open, Db, OpenArgs}, From}], State);
handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call({set_verbose, What}, _From, State) ->
    set_verbose(What),
    {reply, ok, State};
handle_call({users, Db}, _From, State) ->
    Users = ets:select(State#state.store, [{{'$1', Db}, [], ['$1']}]),
    {reply, Users, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({pending_reply, {Ref, Result0}}, State) ->
    {value, #pending{db = Db, pid = Pid, from = {FromPid, _Tag}=From,
                     reqtype = ReqT, clients = Clients}} =
        lists:keysearch(Ref, #pending.ref, State#state.pending),
    Store = State#state.store,
    Result =
	case {Result0, ReqT} of
	    {ok, add_user} ->
		do_link(Store, FromPid),
		true = ets:insert(Store, {FromPid, Db}),
		ets:update_counter(?REGISTRY, Db, 1),
		{ok, Db};
	    {ok, internal_open} ->
		link(Pid),
		do_link(Store, FromPid),
		true = ets:insert(Store, {FromPid, Db}),
                %% do_internal_open() has already done the following:
                %% true = ets:insert(?REGISTRY, {Db, 1, Pid}),
                %% true = ets:insert(?OWNERS, {Pid, Db}),
		{ok, Db};
            {Reply, internal_open} ->
                %% Clean up what do_internal_open() did:
                true = ets:delete(?REGISTRY, Db),
                true = ets:delete(?OWNERS, Pid),
                Reply;
	    {Reply, _} -> % ok or Error
		Reply
	end,
    gen_server:reply(From, Result),
    NP = lists:keydelete(Pid, #pending.pid, State#state.pending),
    State1 = State#state{pending = NP},
    request(Clients, State1);
handle_info({'EXIT', Pid, _Reason}, State) ->
    Store = State#state.store,
    case pid2name_1(Pid) of
        {ok, Db} ->
            %% A dble was killed.
            true = ets:delete(?REGISTRY, Db),
            true = ets:delete(?OWNERS, Pid),
            Users = ets:select(State#state.store, [{{'$1', Db}, [], ['$1']}]),
            true = ets:match_delete(Store, {'_', Db}),
            lists:foreach(fun(User) -> do_unlink(Store, User) end, Users),
            {noreply, State};
        undefined ->
            %% Close all dbles used by Pid.
            F = fun({FromPid, Db}, S) ->
                        {_, S1} = handle_close(S, {close, Db},
                                               {FromPid, notag}, Db),
                        S1
                end,
            State1 = lists:foldl(F, State, ets:lookup(Store, Pid)),
            {noreply, State1}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
terminate(_Reason, _State) ->
    ok.


ensure_started() ->
    case whereis(?SERVER_NAME) of
        undefined ->
            {ok, _} = application:ensure_all_started(cowdb),
            ok;
        _ -> ok
    end.


verbose_flag() ->
    application:get_env(dets, verbose, false).

set_verbose(true) ->
    put(verbose, yes);
set_verbose(_) ->
    erase(verbose).


%% Inlined.
pid2name_1(Pid) ->
    case ets:lookup(?OWNERS, Pid) of
        [] -> undefined;
        [{_Pid,Db}] -> {ok, Db}
    end.


request([{Req, From} | L], State) ->
    Res = case Req of
              {close, Db} ->
                  handle_close(State, Req, From, Db);
              {open, Db, OpenArgs} ->
                  do_open(State, Req, From, OpenArgs, Db)
          end,
    State2 = case Res of
                 {pending, State1} ->
                     State1;
                 {Reply, State1} ->
		     gen_server:reply(From, Reply),
                     State1
             end,
    request(L, State2);
request([], State) ->
    {noreply, State}.

%% -> {pending, NewState} | {Reply, NewState}
do_open(State, Req, From, Args, Db) ->
    case check_pending(Db, From, State, Req) of
        {pending, NewState} -> {pending, NewState};
        false ->
            case ets:lookup(?REGISTRY, Db) of
                [] ->
                    A = [Db, Args, get(verbose)],
                    do_internal_open(State, From, A);
                [{Db, _Counter, Pid}] ->
                    pending_call(Db, Pid, make_ref(), From, Args,
                                 add_user, State)
            end
    end.

%% -> {pending, NewState} | {Reply, NewState}
do_internal_open(State, From, [Db, _, _]=Args) ->
    case supervisor:start_child(cowdb_dbs_sup, [self()]) of
        {ok, Pid} ->
            Ref = make_ref(),
            true = ets:insert(?REGISTRY, {Db, 1, Pid}),
            true = ets:insert(?OWNERS, {Pid, Db}),
            pending_call(Db, Pid, Ref, From, Args, internal_open, State);
        Error ->
            {Error, State}
    end.

%% -> {pending, NewState} | {Reply, NewState}
handle_close(State, Req, {FromPid, _Tag}=From, Db) ->
    case check_pending(Db, From, State, Req) of
        {pending, NewState} -> {pending, NewState};
        false ->
            Store = State#state.store,
            case ets:match_object(Store, {FromPid, Db}) of
                [] ->
                    ?DEBUGF("cowdb: Dadbses ~w close attempt by non-owner~w~n",
                            [Db, FromPid]),
                    {{error, not_owner}, State};
                [_ | Keep] ->
                    case ets:lookup(?REGISTRY, Db) of
                        [{Db, 1, Pid}] ->
                            do_unlink(Store, FromPid),
                            true = ets:delete(?REGISTRY, Db),
                            true = ets:delete(?OWNERS, Pid),
                            true = ets:match_delete(Store, {FromPid, Db}),
                            unlink(Pid),
                            pending_call(Db, Pid, make_ref(), From, [],
                                         internal_close, State);
                        [{Db, _Counter, Pid}] ->
                            do_unlink(Store, FromPid),
                            true = ets:match_delete(Store, {FromPid, Db}),
                            true = ets:insert(Store, Keep),
                            ets:update_counter(?REGISTRY, Db, -1),
                            pending_call(Db, Pid, make_ref(), From, [],
                                         remove_user, State)
                    end
            end
    end.

%% Links with counters
do_link(Store, Pid) ->
    Key = {links, Pid},
    case ets:lookup(Store, Key) of
	[] ->
	    true = ets:insert(Store, {Key, 1}),
	    link(Pid);
	[{_, C}] ->
	    true = ets:delete(Store, Key),
	    true = ets:insert(Store, {Key, C+1})
    end.

do_unlink(Store, Pid) ->
    Key = {links, Pid},
    case ets:lookup(Store, Key) of
	[{_, C}] when C > 1 ->
	    true = ets:delete(Store, Key),
	    true = ets:insert(Store, {Key, C-1});
	_ ->
	    true = ets:delete(Store, Key),
	    unlink(Pid)

    end.

pending_call(Db, Pid, Ref, {FromPid, _Tag}=From, Args, ReqT, State) ->
    Server = self(),
    F = fun() ->
                Res = case ReqT of
                          add_user ->
                              cowdb:add_user(Pid, Db, Args);
                          internal_open ->
                              cowdb:internal_open(Pid, Args);
                          internal_close ->
                              cowdb:internal_close(Pid);
                          remove_user ->
                              cowdb:remove_user(Pid, FromPid)
                      end,
                Server ! {pending_reply, {Ref, Res}}
        end,
    _ = spawn(F),
    PD = #pending{db = Db, ref = Ref, pid = Pid, reqtype = ReqT,
                  from = From, clients = []},
    P = [PD | State#state.pending],
    {pending, State#state{pending = P}}.

check_pending(Db, From, State, Req) ->
    case lists:keysearch(Db, #pending.db, State#state.pending) of
        {value, #pending{db = Db, clients = Clients}=P} ->
            NP = lists:keyreplace(Db, #pending.db, State#state.pending,
                                  P#pending{clients = Clients++[{Req,From}]}),
            {pending, State#state{pending = NP}};
        false ->
            false
    end.
