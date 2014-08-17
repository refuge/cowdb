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

-module(cowdb_file_tests).

-include("cowdb_tests.hrl").

-define(BLOCK_SIZE, 4096).
-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(foreach(Fs), {foreach, fun setup/0, fun teardown/1, Fs}).


setup() ->
    {ok, Fd} = cowdb_file:open(?tempfile(), [create, overwrite]),
    Fd.

teardown(Fd) ->
    ok = cowdb_file:close(Fd).


open_close_test_() ->
    {
        "Test for proper file open and close",
        [
            should_return_enoent_if_missed(),
            should_ignore_invalid_flags_with_open(),
            ?setup(fun should_return_pid_on_file_open/1),
            should_close_file_properly(),
            ?setup(fun should_create_empty_new_files/1)
        ]
    }.

should_return_enoent_if_missed() ->
    ?_assertEqual({error, enoent}, cowdb_file:open("not a real file")).

should_ignore_invalid_flags_with_open() ->
    ?_assertMatch({ok, _},
                  cowdb_file:open(?tempfile(), [create, invalid_option])).

should_return_pid_on_file_open(Fd) ->
    ?_assert(is_pid(Fd)).

should_close_file_properly() ->
    {ok, Fd} = cowdb_file:open(?tempfile(), [create, overwrite]),
    ok = cowdb_file:close(Fd),
    ?_assert(true).

should_create_empty_new_files(Fd) ->
    ?_assertMatch({ok, 0}, cowdb_file:bytes(Fd)).


read_write_test_() ->
    {
        "Common file read/write tests",
        ?foreach([
            fun should_increase_file_size_on_write/1,
            fun should_return_current_file_size_on_write/1,
            fun should_write_and_read_term/1,
            fun should_write_and_read_binary/1,
            fun should_write_and_read_large_binary/1,
            fun should_read_iolist/1,
            fun should_fsync/1,
            fun should_not_read_beyond_eof/1,
            fun should_truncate/1
        ])
    }.


should_increase_file_size_on_write(Fd) ->
    {ok, 0, _} = cowdb_file:append_term(Fd, foo),
    {ok, Size} = cowdb_file:bytes(Fd),
    ?_assert(Size > 0).

should_return_current_file_size_on_write(Fd) ->
    {ok, 0, _} = cowdb_file:append_term(Fd, foo),
    {ok, Size} = cowdb_file:bytes(Fd),
    ?_assertMatch({ok, Size, _}, cowdb_file:append_term(Fd, bar)).

should_write_and_read_term(Fd) ->
    {ok, Pos, _} = cowdb_file:append_term(Fd, foo),
    ?_assertMatch({ok, foo}, cowdb_file:pread_term(Fd, Pos)).

should_write_and_read_binary(Fd) ->
    {ok, Pos, _} = cowdb_file:append_binary(Fd, <<"fancy!">>),
    ?_assertMatch({ok, <<"fancy!">>}, cowdb_file:pread_binary(Fd, Pos)).

should_write_and_read_large_binary(Fd) ->
    BigBin = list_to_binary(lists:duplicate(100000, 0)),
    {ok, Pos, _} = cowdb_file:append_binary(Fd, BigBin),
    ?_assertMatch({ok, BigBin}, cowdb_file:pread_binary(Fd, Pos)).

should_read_iolist(Fd) ->
    %% append_binary == append_iolist?
    %% Possible bug in pread_iolist or iolist() -> append_binary
    {ok, Pos, _} = cowdb_file:append_binary(Fd, ["foo", $m, <<"bam">>]),
    {ok, IoList} = cowdb_file:pread_iolist(Fd, Pos),
    ?_assertMatch(<<"foombam">>, iolist_to_binary(IoList)).

should_fsync(Fd) ->
    {"How does on test fsync?", ?_assertMatch(ok, cowdb_file:sync(Fd))}.

should_not_read_beyond_eof(_) ->
    {"No idea how to test reading beyond EOF", ?_assert(true)}.

should_truncate(Fd) ->
    {ok, 0, _} = cowdb_file:append_term(Fd, foo),
    {ok, Size} = cowdb_file:bytes(Fd),
    BigBin = list_to_binary(lists:duplicate(100000, 0)),
    {ok, _, _} = cowdb_file:append_binary(Fd, BigBin),
    ok = cowdb_file:truncate(Fd, Size),
    ?_assertMatch({ok, foo}, cowdb_file:pread_term(Fd, 0)).


header_test_() ->
    {
        "File header read/write tests",
        [
            ?foreach([
                fun should_write_and_read_atom_header/1,
                fun should_write_and_read_tuple_header/1,
                fun should_write_and_read_second_header/1,
                fun should_truncate_second_header/1,
                fun should_produce_same_file_size_on_rewrite/1,
                fun should_save_headers_larger_than_block_size/1
            ]),
            should_recover_header_marker_corruption(),
            should_recover_header_size_corruption(),
            should_recover_header_crc32sig_corruption(),
            should_recover_header_data_corruption()
        ]
    }.


should_write_and_read_atom_header(Fd) ->
    {ok, HeaderPos} = cowdb_file:write_header(Fd, hello),
    ?_assertMatch({ok, hello, HeaderPos}, cowdb_file:read_header(Fd)).

should_write_and_read_tuple_header(Fd) ->
    {ok, _} = cowdb_file:write_header(Fd, {<<"some_data">>, 32}),
    ?_assertMatch({ok, {<<"some_data">>, 32}, _}, cowdb_file:read_header(Fd)).

should_write_and_read_second_header(Fd) ->
    {ok, 0} = cowdb_file:write_header(Fd, {<<"some_data">>, 32}),
    {ok, 4096} = cowdb_file:write_header(Fd, [foo, <<"more">>]),
    ?_assertMatch({ok, [foo, <<"more">>], 4096}, cowdb_file:read_header(Fd)).

should_truncate_second_header(Fd) ->
    {ok, _} = cowdb_file:write_header(Fd, {<<"some_data">>, 32}),
    {ok, Size} = cowdb_file:bytes(Fd),
    {ok, _} = cowdb_file:write_header(Fd, [foo, <<"more">>]),
    ok = cowdb_file:truncate(Fd, Size),
    ?_assertMatch({ok, {<<"some_data">>, 32}, 0}, cowdb_file:read_header(Fd)).

should_produce_same_file_size_on_rewrite(Fd) ->
    {ok, _} = cowdb_file:write_header(Fd, {<<"some_data">>, 32}),
    {ok, Size1} = cowdb_file:bytes(Fd),
    {ok, _} = cowdb_file:write_header(Fd, [foo, <<"more">>]),
    {ok, Size2} = cowdb_file:bytes(Fd),
    ok = cowdb_file:truncate(Fd, Size1),
    {ok, _} = cowdb_file:write_header(Fd, [foo, <<"more">>]),
    ?_assertMatch({ok, Size2}, cowdb_file:bytes(Fd)).

should_save_headers_larger_than_block_size(Fd) ->
    Header = erlang:make_tuple(5000, <<"CouchDB">>),
    {ok, _} = cowdb_file:write_header(Fd, Header),
    {"COUCHDB-1319", ?_assertMatch({ok, Header, 0},
                                   cowdb_file:read_header(Fd))}.


should_recover_header_marker_corruption() ->
    ?_assertMatch(
        ok,
        check_header_recovery(
            fun(Fd, RawFd, Expect, HeaderPos) ->
                ?assertNotMatch(Expect, cowdb_file:read_header(Fd)),
                file:pwrite(RawFd, HeaderPos, <<0>>),
                ?assertMatch(Expect, cowdb_file:read_header(Fd))
            end)
    ).

should_recover_header_size_corruption() ->
    ?_assertMatch(
        ok,
        check_header_recovery(
            fun(Fd, RawFd, Expect, HeaderPos) ->
                ?assertNotMatch(Expect, cowdb_file:read_header(Fd)),
                % +1 for 0x1 byte marker
                file:pwrite(RawFd, HeaderPos + 1, <<10/integer>>),
                ?assertMatch(Expect, cowdb_file:read_header(Fd))
            end)
    ).

should_recover_header_crc32sig_corruption() ->
    ?_assertMatch(
        ok,
        check_header_recovery(
            fun(Fd, RawFd, Expect, HeaderPos) ->
                ?assertNotMatch(Expect, cowdb_file:read_header(Fd)),
                % +5 = +1 for 0x1 byte and +4 for term size.
                file:pwrite(RawFd, HeaderPos + 5, <<"F01034F88D320B22">>),
                ?assertMatch(Expect, cowdb_file:read_header(Fd))
            end)
    ).

should_recover_header_data_corruption() ->
    ?_assertMatch(
        ok,
        check_header_recovery(
            fun(Fd, RawFd, Expect, HeaderPos) ->
                ?assertNotMatch(Expect, cowdb_file:read_header(Fd)),
                % +9 = +1 for 0x1 byte, +4 for term size and +4 for % CRC32 sig
                file:pwrite(RawFd, HeaderPos + 9, <<"some data goes here!">>),
                ?assertMatch(Expect, cowdb_file:read_header(Fd))
            end)
    ).


check_header_recovery(CheckFun) ->
    Path = ?tempfile(),
    {ok, Fd} = cowdb_file:open(Path, [create, overwrite]),
    {ok, RawFd} = file:open(Path, [read, write, raw, binary]),

    {ok, _} = write_random_data(Fd),
    ExpectHeader = {some_atom, <<"a binary">>, 756},
    {ok, ExpectHeaderPos} = cowdb_file:write_header(Fd, ExpectHeader),

    {ok, HeaderPos} = write_random_data(Fd),
    {ok, _} = cowdb_file:write_header(Fd, {2342, <<"corruption! greed!">>}),

    CheckFun(Fd, RawFd, {ok, ExpectHeader, ExpectHeaderPos}, HeaderPos),

    ok = file:close(RawFd),
    ok = cowdb_file:close(Fd),
    ok.

write_random_data(Fd) ->
    write_random_data(Fd, 100 + random:uniform(1000)).

write_random_data(Fd, 0) ->
    {ok, Bytes} = cowdb_file:bytes(Fd),
    {ok, (1 + Bytes div ?BLOCK_SIZE) * ?BLOCK_SIZE};
write_random_data(Fd, N) ->
    Choices = [foo, bar, <<"bizzingle">>, "bank", ["rough", stuff]],
    Term = lists:nth(random:uniform(4) + 1, Choices),
    {ok, _, _} = cowdb_file:append_term(Fd, Term),
    write_random_data(Fd, N - 1).
