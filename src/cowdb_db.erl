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

%% @doc database module definition

-module(cow_db).

-include("cowdb.hrl").

-type db() :: #db{}.

%% @doc called when a database is initialised, this where different
%% stores (btrees) are created
-callback init(Db :: db()) ->
    {ok, Db2 :: db()}
    | {error, Reason ::any()}.


%% @doc called when a database is closed.
-callback close(Db :: db()) ->
    ok
    | {error, Reason :: any()}.
