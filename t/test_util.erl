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

-module(test_util).

-export([init_code_path/0]).
-export([builddir/0, srcdir/0, depsdir/0, testdir/0]).
-export([source_file/1, build_file/1, test_file/1]).
-export([run/2]).

builddir() ->
    Current = filename:dirname(code:which(?MODULE)),
    filename:absname(filename:join([Current, ".."])).

srcdir() ->
    filename:join([builddir(), "src"]).

depsdir() ->
    filename:join([builddir(), "deps"]).

testdir() ->
    filename:join([builddir(), "t", "out"]).

source_file(Name) ->
    filename:join([srcdir(), Name]).

build_file(Name) ->
    filename:join([builddir(), Name]).

test_file(Name) ->
    filename:join([testdir(), Name]).


init_code_path() ->
    lists:foreach(fun(Name) ->
                code:add_patha(filename:join([depsdir(), Name, "ebin"]))
        end, filelib:wildcard("*", depsdir())),

    code:add_patha(filename:join([builddir(), "ebin"])),

    code:add_patha(filename:join([builddir(), "t"])).


run(Plan, Fun) ->
    test_util:init_code_path(),
    etap:plan(Plan),
    case (catch Fun()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally:~n~p", [Other])),
            timer:sleep(500),
            etap:bail(Other)
    end,
    ok.
