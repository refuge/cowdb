-module(cowdb_dbs_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-spec start_link() -> {'ok', pid()} | 'ignore' | {'error', term()}.

start_link() ->
    supervisor:start_link({local, cowdb_dbs_sup}, cowdb_dbs_sup, []).

-spec init([]) ->
    {'ok', {{'simple_one_for_one', 4, 3600},
            [{'cowdb', {'cowdb', 'istart_link', []},
              'temporary', 30000, 'worker', ['cowdb']}]}}.

init([]) ->
    Db = {cowdb,
             {cowdb, istart_link, []},
             temporary, 30000, worker, [cowdb]},

    {ok, {{simple_one_for_one, 4, 3600}, [Db]}}.
