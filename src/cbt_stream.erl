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
%
%
% @doc module to store a large binary (stream) in the database file and
% get the list of each chunk

-module(cbt_stream).
-behaviour(gen_server).

% public API
-export([open/1, open/2, close/1]).
-export([foldl/4, foldl/5, foldl_decode/6, range_foldl/6]).
-export([copy_to_new_stream/3, write/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_cast/2, handle_call/3, handle_info/2]).

-include("cbt.hrl").

-define(DEFAULT_BUFFER_SIZE, 4096).

-record(stream,
    {fd = 0,
    written_pointers=[],
    buffer_list = [],
    buffer_len = 0,
    max_buffer,
    written_len = 0,
    md5,
    % md5 of the content without any transformation applied (e.g. compression)
    % needed for the attachment upload integrity check (ticket 558)
    identity_md5,
    identity_len = 0,
    encoding_fun,
    end_encoding_fun
    }).

-type cbt_stream() :: pid().
-type cbt_stream_options() :: [encoding | {compression_level, integer()}
                               | {buffer_size, integer()}].
-export_type([cbt_stream/0]).

%%% Interface functions %%%

%% @doc open a new stream
-spec open(Fd::cbt_file:cbt_file()) -> {ok, cbt_stream()}.
open(Fd) ->
    open(Fd, []).

%% @doc open a new stream
-spec open(Fd::cbt_file:cbt_file(), Options::cbt_stream_options())
    -> {ok, cbt_stream()}.
open(Fd, Options) ->
    gen_server:start_link(cbt_stream, {Fd, Options}, []).

%% @doc close the stream
-spec close(Stream::cbt_stream()) -> ok.
close(Pid) ->
    gen_server:call(Pid, close, infinity).

%% @doc copy a stream from one file to another
-spec copy_to_new_stream(Fd::cbt_stream(), PosList::[integer()],
                         DestFd::cbt_stream()) -> ok | {error, term()}.
copy_to_new_stream(Fd, PosList, DestFd) ->
    {ok, Dest} = open(DestFd),
    foldl(Fd, PosList,
        fun(Bin, _) ->
            ok = write(Dest, Bin)
        end, ok),
    close(Dest).

%% @doc retrieve all chunks from a list of their positions in the file.
%% Results are passed to a function:
%%
%% ```
%% fun(Chunk, Acc) -> Acc2
%% '''
-spec foldl(Fd::cbt_stream(), PosList::[integer()], Fun::fun(), Acc::any()) -> Acc2::any().
foldl(_Fd, [], _Fun, Acc) ->
    Acc;
foldl(Fd, [Pos|Rest], Fun, Acc) ->
    {ok, Bin} = cbt_file:pread_iolist(Fd, Pos),
    foldl(Fd, Rest, Fun, Fun(Bin, Acc)).

%% @doc like `fold/4' but check the signature.
%%
-spec foldl(Fd::cbt_stream(), PosList::[integer()], Md5::binary(),
            Fun::fun(), Acc::any()) -> Acc2::any().
foldl(Fd, PosList, <<>>, Fun, Acc) ->
    foldl(Fd, PosList, Fun, Acc);
foldl(Fd, PosList, Md5, Fun, Acc) ->
    foldl(Fd, PosList, Md5, cbt_util:md5_init(), Fun, Acc).

%% @doc same as fold but decode the chunk if needed.
-spec foldl_decode(Fd::cbt_stream(), PosList::[integer()], Md5::binary(),
       Encoding::gzip | identity, Fun::fun(), Acc::any()) -> Acc2::any().
foldl_decode(Fd, PosList, Md5, Enc, Fun, Acc) ->
    {DecDataFun, DecEndFun} = case Enc of
    gzip ->
        ungzip_init();
    identity ->
        identity_enc_dec_funs()
    end,
    Result = foldl_decode(
        DecDataFun, Fd, PosList, Md5, cbt_util:md5_init(), Fun, Acc
    ),
    DecEndFun(),
    Result.

foldl(_Fd, [], Md5, Md5Acc, _Fun, Acc) ->
    Md5 = cbt_util:md5_final(Md5Acc),
    Acc;
foldl(Fd, [{Pos, _Size}], Md5, Md5Acc, Fun, Acc) -> % 0110 UPGRADE CODE
    foldl(Fd, [Pos], Md5, Md5Acc, Fun, Acc);
foldl(Fd, [Pos], Md5, Md5Acc, Fun, Acc) ->
    {ok, Bin} = cbt_file:pread_iolist(Fd, Pos),
    Md5 = cbt_util:md5_final(cbt_util:md5_update(Md5Acc, Bin)),
    Fun(Bin, Acc);
foldl(Fd, [{Pos, _Size}|Rest], Md5, Md5Acc, Fun, Acc) ->
    foldl(Fd, [Pos|Rest], Md5, Md5Acc, Fun, Acc);
foldl(Fd, [Pos|Rest], Md5, Md5Acc, Fun, Acc) ->
    {ok, Bin} = cbt_file:pread_iolist(Fd, Pos),
    foldl(Fd, Rest, Md5, cbt_util:md5_update(Md5Acc, Bin), Fun, Fun(Bin, Acc)).


%% @doc retrieve all chunks in a range.
-spec range_foldl(Fd::cbt_stream(), PosList::[integer()],
                  From::integer(), To::integer(), Fun::fun(), Acc::any())
    -> Acc2::any().
range_foldl(Fd, PosList, From, To, Fun, Acc) ->
    range_foldl(Fd, PosList, From, To, 0, Fun, Acc).

range_foldl(_Fd, _PosList, _From, To, Off, _Fun, Acc) when Off >= To ->
    Acc;
range_foldl(Fd, [Pos|Rest], From, To, Off, Fun, Acc) when is_integer(Pos) -> % old-style attachment
    {ok, Bin} = cbt_file:pread_iolist(Fd, Pos),
    range_foldl(Fd, [{Pos, iolist_size(Bin)}] ++ Rest, From, To, Off, Fun, Acc);
range_foldl(Fd, [{_Pos, Size}|Rest], From, To, Off, Fun, Acc) when From > Off + Size ->
    range_foldl(Fd, Rest, From, To, Off + Size, Fun, Acc);
range_foldl(Fd, [{Pos, Size}|Rest], From, To, Off, Fun, Acc) ->
    {ok, Bin} = cbt_file:pread_iolist(Fd, Pos),
    Bin1 = if
        From =< Off andalso To >= Off + Size -> Bin; %% the whole block is covered
        true ->
            PrefixLen = clip(From - Off, 0, Size),
            PostfixLen = clip(Off + Size - To, 0, Size),
            MatchLen = Size - PrefixLen - PostfixLen,
            <<_Prefix:PrefixLen/binary,Match:MatchLen/binary,_Postfix:PostfixLen/binary>> = iolist_to_binary(Bin),
            Match
    end,
    range_foldl(Fd, Rest, From, To, Off + Size, Fun, Fun(Bin1, Acc)).

clip(Value, Lo, Hi) ->
    if
        Value < Lo -> Lo;
        Value > Hi -> Hi;
        true -> Value
    end.

foldl_decode(_DecFun, _Fd, [], Md5, Md5Acc, _Fun, Acc) ->
    Md5 = cbt_util:md5_final(Md5Acc),
    Acc;
foldl_decode(DecFun, Fd, [{Pos, _Size}], Md5, Md5Acc, Fun, Acc) ->
    foldl_decode(DecFun, Fd, [Pos], Md5, Md5Acc, Fun, Acc);
foldl_decode(DecFun, Fd, [Pos], Md5, Md5Acc, Fun, Acc) ->
    {ok, EncBin} = cbt_file:pread_iolist(Fd, Pos),
    Md5 = cbt_util:md5_final(cbt_util:md5_update(Md5Acc, EncBin)),
    Bin = DecFun(EncBin),
    Fun(Bin, Acc);
foldl_decode(DecFun, Fd, [{Pos, _Size}|Rest], Md5, Md5Acc, Fun, Acc) ->
    foldl_decode(DecFun, Fd, [Pos|Rest], Md5, Md5Acc, Fun, Acc);
foldl_decode(DecFun, Fd, [Pos|Rest], Md5, Md5Acc, Fun, Acc) ->
    {ok, EncBin} = cbt_file:pread_iolist(Fd, Pos),
    Bin = DecFun(EncBin),
    Md5Acc2 = cbt_util:md5_update(Md5Acc, EncBin),
    foldl_decode(DecFun, Fd, Rest, Md5, Md5Acc2, Fun, Fun(Bin, Acc)).

gzip_init(Options) ->
    case cbt_util:get_value(compression_level, Options, 0) of
    Lvl when Lvl >= 1 andalso Lvl =< 9 ->
        Z = zlib:open(),
        % 15 = ?MAX_WBITS (defined in the zlib module)
        % the 16 + ?MAX_WBITS formula was obtained by inspecting zlib:gzip/1
        ok = zlib:deflateInit(Z, Lvl, deflated, 16 + 15, 8, default),
        {
            fun(Data) ->
                zlib:deflate(Z, Data)
            end,
            fun() ->
                Last = zlib:deflate(Z, [], finish),
                ok = zlib:deflateEnd(Z),
                ok = zlib:close(Z),
                Last
            end
        };
    _ ->
        identity_enc_dec_funs()
    end.

ungzip_init() ->
    Z = zlib:open(),
    zlib:inflateInit(Z, 16 + 15),
    {
        fun(Data) ->
            zlib:inflate(Z, Data)
        end,
        fun() ->
            ok = zlib:inflateEnd(Z),
            ok = zlib:close(Z)
        end
    }.

identity_enc_dec_funs() ->
    {
        fun(Data) -> Data end,
        fun() -> [] end
    }.


%% @doc write a chunk from the stream on the database file.
-spec write(Stream::cbt_stream(), Bin::binary()) -> ok | {error, term()}.
write(_Pid, <<>>) ->
    ok;
write(Pid, Bin) ->
    gen_server:call(Pid, {write, Bin}, infinity).



%% @private
init({Fd, Options}) ->
    {EncodingFun, EndEncodingFun} =
    case cbt_util:get_value(encoding, Options, identity) of
    identity ->
        identity_enc_dec_funs();
    gzip ->
        gzip_init(Options)
    end,
    {ok, #stream{
            fd=Fd,
            md5=cbt_util:md5_init(),
            identity_md5=cbt_util:md5_init(),
            encoding_fun=EncodingFun,
            end_encoding_fun=EndEncodingFun,
            max_buffer=cbt_util:get_value(
                buffer_size, Options, ?DEFAULT_BUFFER_SIZE)
        }
    }.

%% @private
terminate(_Reason, _Stream) ->
    ok.

%% @private
handle_call({write, Bin}, _From, Stream) ->
    BinSize = iolist_size(Bin),
    #stream{
        fd = Fd,
        written_len = WrittenLen,
        written_pointers = Written,
        buffer_len = BufferLen,
        buffer_list = Buffer,
        max_buffer = Max,
        md5 = Md5,
        identity_md5 = IdenMd5,
        identity_len = IdenLen,
        encoding_fun = EncodingFun} = Stream,
    if BinSize + BufferLen > Max ->
            WriteBin = lists:reverse(Buffer, [Bin]),
            IdenMd5_2 = cbt_util:md5_update(IdenMd5, WriteBin),
            {WrittenLen2, Md5_2, Written2} = case EncodingFun(WriteBin) of
                [] ->
                    % case where the encoder did some internal buffering
                    % (zlib does it for example)
                    {WrittenLen, Md5, Written};
                WriteBin2 ->
                    {ok, Pos, _} = cbt_file:append_binary(Fd, WriteBin2),
                    WrittenLen1 = WrittenLen + iolist_size(WriteBin2),
                    Md5_1 = cbt_util:md5_update(Md5, WriteBin2),
                    Written1 = [{Pos, iolist_size(WriteBin2)}|Written],
                    {WrittenLen1, Md5_1, Written1}
            end,

            {reply, ok, Stream#stream{
                    written_len=WrittenLen2,
                    written_pointers=Written2,
                    buffer_list=[],
                    buffer_len=0,
                    md5=Md5_2,
                    identity_md5=IdenMd5_2,
                    identity_len=IdenLen + BinSize}};
        true ->
            {reply, ok, Stream#stream{
                    buffer_list=[Bin|Buffer],
                    buffer_len=BufferLen + BinSize,
                    identity_len=IdenLen + BinSize}}
    end;
handle_call(close, _From, Stream) ->
    #stream{
        fd = Fd,
        written_len = WrittenLen,
        written_pointers = Written,
        buffer_list = Buffer,
        md5 = Md5,
        identity_md5 = IdenMd5,
        identity_len = IdenLen,
        encoding_fun = EncodingFun,
        end_encoding_fun = EndEncodingFun} = Stream,

    WriteBin = lists:reverse(Buffer),
    IdenMd5Final = cbt_util:md5_final(cbt_util:md5_update(IdenMd5, WriteBin)),
    WriteBin2 = EncodingFun(WriteBin) ++ EndEncodingFun(),
    Md5Final = cbt_util:md5_final(cbt_util:md5_update(Md5, WriteBin2)),
    Result = case WriteBin2 of
    [] ->
        {lists:reverse(Written), WrittenLen, IdenLen, Md5Final, IdenMd5Final};
    _ ->
        {ok, Pos, _} = cbt_file:append_binary(Fd, WriteBin2),
        StreamInfo = lists:reverse(Written, [{Pos, iolist_size(WriteBin2)}]),
        StreamLen = WrittenLen + iolist_size(WriteBin2),
        {StreamInfo, StreamLen, IdenLen, Md5Final, IdenMd5Final}
    end,
    {stop, normal, Result, Stream}.

%% @private
handle_cast(_Msg, State) ->
    {noreply,State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.
