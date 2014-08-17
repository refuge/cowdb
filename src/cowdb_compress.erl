%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
%% This file incorporates work covered by the following copyright and
%% permission notice:
%%
%%   Copyright 2007-2012 - The Apache Software foundation
%%   Copyright 2011-2014 - Couchbase
%%   Copyright 2014 - Benoit Chesneau & The Cowdb Authors
%%
%%   Licensed under the Apache License, Version 2.0 (the "License"); you
%%   may not use this file except in compliance with the License. You
%%   may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%%  implied. See the License for the specific language governing
%%  permissions and limitations under the License.

-module(cowdb_compress).

-export([compress/2, decompress/1, is_compressed/2]).

-include("cowdb.hrl").

-define(NO_COMPRESSION, 0).
% binaries compressed with snappy have their first byte set to this value
-define(SNAPPY_PREFIX, 1).
% binaries compressed with gzip have their first byte set to this value
-define(GZIP_PREFIX, 2).
% binaries compressed with lz4 have their first byte set to this value
-define(LZ4_PREFIX, 3).
% Term prefixes documented at:
%      http://www.erlang.org/doc/apps/erts/erl_ext_dist.html
-define(TERM_PREFIX, 131).
-define(COMPRESSED_TERM_PREFIX, 131, 80).

-type compression_method() :: snappy | lz4 | gzip | none.
-export_type([compression_method/0]).


use_compressed(UncompressedSize, CompressedSize)
        when CompressedSize < UncompressedSize ->
    true;
use_compressed(_UncompressedSize, _CompressedSize) ->
    false.


%% @doc compress an encoded binary with the following type. When an
%% erlang term is given it is encoded to a binary.
-spec compress(Bin::binary()|term(), Method::compression_method()) -> Bin::binary().
compress(Term, snappy) ->
    Bin = ?term_to_bin(Term),
    {ok, CompressedBin} = snappy:compress(Bin),
    case use_compressed(byte_size(Bin), byte_size(CompressedBin)) of
        true ->
            <<?SNAPPY_PREFIX, CompressedBin/binary>>;
        false ->
            << ?NO_COMPRESSION, Bin/binary >>
    end;
compress(Term, gzip) ->
    Bin = ?term_to_bin(Term),
    {ok, CompressedBin} = zlib:gzip(Bin),
    case use_compressed(byte_size(Bin), byte_size(CompressedBin)) of
        true ->
            <<?GZIP_PREFIX, CompressedBin/binary>>;
        false ->
            << ?NO_COMPRESSION, Bin/binary >>
    end;
compress(Term, lz4) ->
    Bin = ?term_to_bin(Term),
    {ok, CompressedBin} = lz4:compress(erlang:iolist_to_binary(Bin)),
    case use_compressed(byte_size(Bin), byte_size(CompressedBin)) of
        true ->
            <<?NO_COMPRESSION, CompressedBin/binary >>;
        false ->
           << ?NO_COMPRESSION, Bin/binary >>
    end;
compress(Term, none) ->
    Bin = ?term_to_bin(Term),
    << ?NO_COMPRESSION, Bin/binary >>.

%% @doc decompress a binary to an erlang decoded term.
-spec decompress(Bin::binary()) -> Term::term().

decompress(<<?NO_COMPRESSION, TermBin/binary >>) ->
    binary_to_term(TermBin);
decompress(<<?SNAPPY_PREFIX, Rest/binary>>) ->
    {ok, TermBin} = snappy:decompress(Rest),
    binary_to_term(TermBin);
decompress(<<?GZIP_PREFIX, Rest/binary>>) ->
    TermBin = zlib:gunzip(Rest),
    binary_to_term(TermBin);
decompress(<<?LZ4_PREFIX, Rest/binary>>) ->
    TermBin = lz4:uncompress(Rest),
    binary_to_term(TermBin).

%% @doc check if the binary has been compressed.
-spec is_compressed(Bin::binary()|term(),
                    Method::compression_method()) -> true | false.
is_compressed(<<?NO_COMPRESSION, _/binary>>, none) ->
    false;
is_compressed(<<?SNAPPY_PREFIX, _/binary>>, snappy) ->
    true;
is_compressed(<<?GZIP_PREFIX, _/binary>>, gzip) ->
    true;
is_compressed(<<?LZ4_PREFIX, _/binary>>, lz4) ->
    true;
is_compressed(_Term, _Method) ->
    false.
