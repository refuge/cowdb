-module(cowdb).

-export([start/0, stop/0]).
-export([open_db/2, close_db/1, verbose/0, verbose/1]).


%% internal methods
-export([istart_link/1, add_user/3, remove_user/2, internal_open/2,
         internal_close/1, init/2, system_continue/3, system_terminate/4,
         system_code_change/4]).


-include("cowdb.hrl").


start() ->
    cowdb_server:start().

stop() ->
    cowdb_server:stop().


open_db(Db, Args) ->
    case catch defaults(Db, Args) of
        OpenArgs when is_record(OpenArgs, open_args) ->
            case cowdb_server:open_db(Db, OpenArgs) of
                badarg -> % Should not happen.
                    erlang:error(cowdb_process_died, [Db, Args]);
                Reply ->
                   Reply
            end;
	_ ->
	    erlang:error(badarg, [Db, Args])
    end.

close_db(Db) ->
    case cowdb_server:close_db(Db) of
        badarg ->
            {error, not_owner};
        Reply ->
            Reply
    end.


verbose() ->
    verbose(true).

verbose(What) ->
    ok = cowdb_server:verbose(What),
    All = cowdb_server:all(),
    lists:foreach(fun(Db) ->
                          dbreq(Db, {set_verbose, What})
                  end, All),
    All.

%%-----------------------------------------------------------------
%% internal API
%%-----------------------------------------------------------------


add_user(Pid, Tab, Args) ->
    req(Pid, {add_user, Tab, Args}).

remove_user(Pid, From) ->
    req(Pid, {close, From}).


internal_open(Pid, Args) ->
    req(Pid, {internal_open,Args}).

internal_close(Pid) ->
    req(Pid, close).


dbreq(Db, R) ->
    case catch cowdb_server:get_pid(Db) of
        Pid when is_pid(Pid) ->
            req(Pid, R);
        _ ->
            badarg
    end.

req(Proc, R) ->
    Ref = erlang:monitor(process, Proc),
    Proc ! ?COWDB_CALL(self(), R),
    receive
        {'DOWN', Ref, process, Proc, _Info} ->
            badarg;
        {Proc, Reply} ->
            erlang:demonitor(Ref, [flush]),
            Reply
    end.


istart_link(Server) ->
    {ok, proc_lib:spawn_link(cowdb, init, [self(), Server])}.


init(Parent, Server) ->
    db_loop(#db{parent=Parent, server=Server}).

db_loop(Db) ->
    receive
        ?COWDB_CALL(From, Op) ->
           do_apply_op(Op, From, Db);
        {'EXIT', Pid, Reason} when Pid =:= Db#db.parent ->
            _NewDb = do_stop(Db),
            exit(Reason);
        {'EXIT', Pid, Reason} when Pid =:= Db#db.server ->
            %% The server is gone.
            _NewDb = do_stop(Db),
            exit(Reason);
        {'EXIT', Pid, _Reason} ->
            %% Compaction process exited
            Db2 = remove_compactor(Db, Pid, close),
            db_loop(Db2);
        {system, From, Req} ->
            sys:handle_system_msg(Req, From, Db#db.parent, ?MODULE, [], Db);
        Message ->
            error_logger:format("** cowdb: unexpected message"
                                "(ignored): ~w~n", [Message]),
            db_loop(Db)
    end.


do_apply_op(Op, From, Db) ->
    try apply_op(Op, From, Db) of
        ok ->
            db_loop(Db);
        Db2 when is_record(Db2, db) ->
            db_loop(Db2)
    catch
        exit:normal ->
            exit(normal);
        _:Error ->
            Name = Db#db.name,
            case cowdb_util:debug_mode() of
                true ->
                    error_logger:format
                      ("** cowdb: Bug was found when accessing db ~w,~n"
                       "** cowdb: operation was ~p and reply was ~w.~n"
                       "** cowdb: Stacktrace: ~w~n",
                       [Name, Op, Error, erlang:get_stacktrace()]);
                false ->
                    error_logger:format
                      ("** cowdb: Bug was found when accessing db ~w~n",
                       [Name])
            end,
            if
                From =/= self() ->
                    From ! {self(), {error, {cowdb_bug, Name, Op, Error}}},
                    ok;
                true ->
                    ok
            end,
            db_loop(Db)
    end.

apply_op(Op, From, Db) ->
    case Op of
        {add_user, DbName, OpenArgs} ->
             #open_args{file = Fname,
                        mode = Mode} = OpenArgs,
             Res = if
                       DbName =:= Db#db.name,
                       Db#db.mode =:= Mode,
                       Db#db.file =:= Fname ->
                           ok;
                       true ->
                           err({error, incompatible_arguments})
                   end,
             From ! {self(), Res},
             ok;

        close  ->
	        From ! {self(), fclose(Db)},
	        exit(normal);
        {close, _Pid} ->
            %% Used from cowsb_server when Pid has closed the database,
            %% but the database is still opened by some process.
            From ! {self(), status(Db)},
            Db;
        {internal_open, Args} ->
            case do_open_file(Args, Db) of
                {ok, Db2} ->
                    From ! {self(), ok},
                    Db2;
                Error ->
                    From ! {self(), Error},
                    exit(normal)
            end;
        {set_verbose, What} ->
            cowdb_util:set_verbose(What),
            From ! {self(), ok},
            ok
    end.


%%-----------------------------------------------------------------
%% Callback functions for system messages handling.
%%-----------------------------------------------------------------
system_continue(_Parent, _, Db) ->
    db_loop(Db).

system_terminate(Reason, _Parent, _, Db) ->
    _NewDb = do_stop(Db),
    exit(Reason).

%%-----------------------------------------------------------------
%% Code for upgrade.
%%-----------------------------------------------------------------
system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.


%%-----------------------------------------------------------------
%% internal functions
%%-----------------------------------------------------------------


%% Process the args list as provided to open_db/2.
defaults(Tab, Args) ->
    Defaults0 = #open_args{file = to_list(Tab),
                           mode = live},
    Fun = fun repl/2,
    lists:foldl(Fun, Defaults0, Args).

to_list(T) when is_atom(T) -> atom_to_list(T);
to_list(T) -> T.


repl({mode, M}, Defs) ->
    m(M, [snapshot, live]),
    Defs#open_args{mode = M};
repl({file, File}, Defs) ->
    Defs#open_args{file = to_list(File)};
repl({_, _}, _) ->
    exit(badarg).

maybe_put(_, undefined) ->
    ignore;
maybe_put(K, V) ->
    put(K, V).


do_open_file([Name, OpenArgs, Verbose], Db) ->
    case cowdb_file:open(OpenArgs#open_args.file, [create_if_missing]) of
        {ok, Fd} ->
            maybe_put(verbose, Verbose),
            {ok, Db#db{name=Name,
                       fd=Fd,
                       file=OpenArgs#open_args.file,
                       mode= live}};
        Error ->
            Error
    end.


m(X, L) ->
    case lists:member(X, L) of
        true -> true;
        false -> exit(badarg)
    end.



do_stop(Db) ->
    fclose(Db).

fclose(#db{fd=Fd}=_Db) ->
    cowdb_file:close(Fd).

status(Db) ->
    case Db#db.update_mode of
        saved -> ok;
        dirty -> ok;
        compacting -> ok;
        Error -> Error
    end.

remove_compactor(_Db, _Pid, _Reason) ->
    ok.



err(Error) ->
    case get(verbose) of
	yes ->
	    error_logger:format("** cowdb: failed with ~w~n", [Error]),
	    Error;
	undefined  ->
	    Error
    end.
