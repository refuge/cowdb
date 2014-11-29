-module(cowdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("cowdb.hrl").



%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    DBsSup = {cowdb_dbs_sup, {cowdb_dbs_sup, start_link, []}, permanent,
              1000, supervisor, [cowdb_dbs_sup]},
    Server = {?SERVER_NAME, {cowdb_server, start_link, []},
              permanent, 2000, worker, [cowdb_server]},
    {ok, { {one_for_one, 5, 10}, [DBsSup, Server]} }.

