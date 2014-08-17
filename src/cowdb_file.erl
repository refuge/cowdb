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

-module(cowdb_file).
-behaviour(gen_server).

-include("cowdb.hrl").

-define(SIZE_BLOCK, 16#1000). % 4 KiB
-define(RETRY_TIME_MS, 1000).
-define(MAX_RETRY_TIME_MS, 10000).

-record(file, {
    fd,
    eof = 0,
    file_path,
    open_options
}).

% public API
-export([open/1, open/2, close/1, bytes/1, sync/1, truncate/2, rename/2,
        reopen/1]).
-export([pread_term/2, pread_iolist/2, pread_binary/2]).
-export([append_binary/2, append_binary_crc32/2]).
-export([append_raw_chunk/2, assemble_file_chunk/1, assemble_file_chunk/2]).
-export([append_term/2, append_term/3, append_term_crc32/2,
         append_term_crc32/3]).
-export([write_header/2, read_header/1]).
-export([delete/2, delete/3, nuke_dir/2, init_delete_dir/1]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-ifdef(DEBUG).
-define(log(Fmt,Args), io:format(Fmt, Args)).
-else.
-define(log(Fmt,Args), ok).
-endif.


-type cowdb_file() :: pid().
-type file_option() :: create | overwrite.
-type file_options() :: [file_option()].
-type append_options() :: [{compression, cowdb_compress:compression_method()}].

-export_type([cowdb_file/0]).
-export_type([file_option/0, file_options/0]).
-export_type([append_options/0]).

%% @doc open a file in a gen_server that will be used to handle btree
%% I/Os.
-spec open(FilePath::string()) -> {ok, cowdb_file()} | {error, term()}.
open(FilePath) ->
    open(FilePath, []).


-spec open(FilePath::string(), Options::file_options())
    -> {ok, cowdb_file()} | {error, term()}.
open(FilePath, Options) ->
    proc_lib:start_link(?MODULE, init, [{FilePath, Options}]).


%% @doc append an Erlang term to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to
%%  the beginning the serialized  term. Use pread_term to read the term
%%  back.
%%  or {error, Reason}.
-spec append_term(Fd::cowdb_file(), Term::term()) ->
    {ok, Pos::integer(), NumBytesWriiten::integer}
    | {error, term}.
append_term(Fd, Term) ->
    append_term(Fd, Term, []).


-spec append_term(Fd::cowdb_file(), Term::term(),
                  Options::append_options()) ->
    {ok, Pos::integer(), NumBytesWriiten::integer}
    | {error, term}.
append_term(Fd, Term, Options) ->
    Comp = cowdb_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
    append_binary(Fd, cowdb_compress:compress(Term, Comp)).


%% @doc append an Erlang term to the end of the file and sign with an
%% crc32 prefix.
-spec append_term_crc32(Fd::cowdb_file(), Term::term()) ->
    {ok, Pos::integer(), NumBytesWriiten::integer}
    | {error, term}.
append_term_crc32(Fd, Term) ->
    append_term_crc32(Fd, Term, []).

-spec append_term_crc32(Fd::cowdb_file(), Term::term(),
                      Options::append_options()) ->
    {ok, Pos::integer(), NumBytesWriiten::integer}
    | {error, term}.
append_term_crc32(Fd, Term, Options) ->
    Comp = cowdb_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
    append_binary_crc32(Fd, cowdb_compress:compress(Term, Comp)).

%% @doc append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to the
%%  beginning the serialized term. Use pread_term to read the term back.
%%  or {error, Reason}.
-spec append_binary(Fd::cowdb_file(), Bin::binary()) ->
    {ok, Pos::integer(), NumBytesWriiten::integer}
    | {error, term}.
append_binary(Fd, Bin) ->
    gen_server:call(Fd, {append_bin, assemble_file_chunk(Bin)}, infinity).

%% @doc append an Erlang binary to the end of the file and sign in with
%% crc32.
-spec append_binary_crc32(Fd::cowdb_file(), Bin::binary()) ->
    {ok, Pos::integer(), NumBytesWriiten::integer}
    | {error, term}.
append_binary_crc32(Fd, Bin) ->
    gen_server:call(Fd,
        {append_bin, assemble_file_chunk(Bin, erlang:crc32(Bin))}, infinity).


%% @doc like append_binary but wihout manipulating the binary, it is
%% stored as is.
-spec append_raw_chunk(Fd::cowdb_file(), Bin::binary()) ->
    {ok, Pos::integer(), NumBytesWriiten::integer}
    | {error, term}.
append_raw_chunk(Fd, Chunk) ->
    gen_server:call(Fd, {append_bin, Chunk}, infinity).


assemble_file_chunk(Bin) ->
    [<<0:1/integer, (iolist_size(Bin)):31/integer>>, Bin].

assemble_file_chunk(Bin, Crc32) ->
    [<<1:1/integer, (iolist_size(Bin)):31/integer, Crc32:32/integer>>, Bin].

%% @doc Reads a term from a file that was written with append_term
%% Args:    Pos, the offset into the file where the term is serialized.
-spec pread_term(Fd::cowdb_file(), Pos::integer()) ->
    {ok, Term::term()} | {error, term()}.
pread_term(Fd, Pos) ->
    {ok, Bin} = pread_binary(Fd, Pos),
    {ok, cowdb_compress:decompress(Bin)}.


%% @doc: Reads a binrary from a file that was written with append_binary
%% Args:    Pos, the offset into the file where the term is serialized.
-spec pread_binary(Fd::cowdb_file(), Pos::integer()) ->
    {ok, Bin::binary()} | {error, term()}.
pread_binary(Fd, Pos) ->
    {ok, L} = pread_iolist(Fd, Pos),
    {ok, iolist_to_binary(L)}.


pread_iolist(Fd, Pos) ->
    case gen_server:call(Fd, {pread_iolist, Pos}, infinity) of
    {ok, IoList, <<>>} ->
        {ok, IoList};
    {ok, IoList, <<Crc32:32/integer>>} ->
        case erlang:crc32(IoList) of
        Crc32 ->
            {ok, IoList};
        _ ->
            error_logger:info_msg("File corruption in ~p at position ~B",
                                  [Fd, Pos]),
            exit({file_corruption, <<"file corruption">>})
        end;
    Error ->
        Error
    end.

%% @doc get he length of a file, in bytes.
-spec bytes(Fd::cowdb_file()) -> {ok, Bytes::integer()} | {error, term()}.
bytes(Fd) ->
    gen_server:call(Fd, bytes, infinity).

%% @doc Truncate a file to the number of bytes.
-spec truncate(Fd::cowdb_file(), Pos::integer()) -> ok | {error, term()}.
truncate(Fd, Pos) ->
    gen_server:call(Fd, {truncate, Pos}, infinity).

%% @doc rename a file safely
-spec rename(Fd::cowdb_file(), NewFilePath::string()) -> ok | {error,term()}.
rename(Fd, NewFilePath) ->
    gen_server:call(Fd, {rename, NewFilePath}, infinity).

%% @doc Ensure all bytes written to the file are flushed to disk.
-spec sync(FdOrPath::cowdb_file()|string()) -> ok | {error, term()}.
sync(FilePath) when is_list(FilePath) ->
    {ok, Fd} = file:open(FilePath, [append, raw]),
    try ok = file:sync(Fd) after ok = file:close(Fd) end;
sync(Fd) ->
    gen_server:call(Fd, sync, infinity).

%% @doc reopen a file
-spec reopen(cowdb_file()) -> ok | {error, term()}.
reopen(Fd) ->
    gen_server:call(Fd, reopen, infinity).


%% @doc Close the file.
-spec close(Fd::cowdb_file()) -> ok.
close(Fd) ->
    try
        gen_server:call(Fd, close, infinity)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.



%% @doc delete a file synchronously.
%% Root dir is the root where to find the file. This call is blocking
%% until the file is deleted.
-spec delete(RootDir::string(), FilePath::string()) -> ok | {error, term()}.
delete(RootDir, FilePath) ->
    delete(RootDir, FilePath, true).

%% @doc delete a file asynchronously or not
-spec delete(RootDir::string(), FilePath::string(), Async::boolean()) ->
    ok | {error, term()}.
delete(RootDir, FilePath, Async) ->
    DelFile = filename:join([RootDir,".delete", cowdb_util:uniqid()]),
    case file:rename(FilePath, DelFile) of
    ok ->
        if (Async) ->
            spawn(file, delete, [DelFile]),
            ok;
        true ->
            file:delete(DelFile)
        end;
    Error ->
        Error
    end.


%% @doc utility function to remove completely the content of a directory
-spec nuke_dir(RootDelDir::string(), Dir::string()) -> ok.
nuke_dir(RootDelDir, Dir) ->
    FoldFun = fun(File) ->
        Path = Dir ++ "/" ++ File,
        case filelib:is_dir(Path) of
            true ->
                ok = nuke_dir(RootDelDir, Path),
                file:del_dir(Path);
            false ->
                delete(RootDelDir, Path, false)
        end
    end,
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:foreach(FoldFun, Files),
            ok = file:del_dir(Dir);
        {error, enoent} ->
            ok
    end.

%% @doc utility function to init the deletion directory where the
%% deleted files will be temporarely stored.
-spec init_delete_dir(RootDir::string()) -> ok.
init_delete_dir(RootDir) ->
    Dir = filename:join(RootDir,".delete"),
    % note: ensure_dir requires an actual filename companent, which is the
    % reason for "foo".
    filelib:ensure_dir(filename:join(Dir,"foo")),
    filelib:fold_files(Dir, ".*", true,
        fun(Filename, _) ->
            ok = file:delete(Filename)
        end, ok).


%% @doc read the database header from the database file
-spec read_header(Fd::cowdb_file())
    -> {ok, Header::term(), Pos::integer()} | {error, term()}.
read_header(Fd) ->
    case gen_server:call(Fd, find_header, infinity) of
    {ok, Bin, Pos} ->
        {ok, binary_to_term(Bin), Pos};
    Else ->
        Else
    end.

%% @doc write the database header at the end of the the database file
-spec write_header(Fd::cowdb_file(), Header::term())
    -> {ok, Pos::integer()} | {error, term()}.
write_header(Fd, Data) ->
    Bin = term_to_binary(Data),
    Crc32 = erlang:crc32(Bin),
    % now we assemble the final header binary and write to disk
    FinalBin = <<Crc32:32/integer, Bin/binary>>,
    gen_server:call(Fd, {write_header, FinalBin}, infinity).


% server functions

init({FilePath, Options}) ->
    ok = maybe_create_file(FilePath, Options),
    case file:read_file_info(FilePath) of
        {ok, _} ->
            OpenOptions = case lists:member(read_only, Options) of
                true -> [binary, read, raw];
                false -> [binary, read, append, raw]
            end,
            case try_open_fd(FilePath, OpenOptions, ?RETRY_TIME_MS,
                             ?MAX_RETRY_TIME_MS) of
                {ok, Fd} ->
                    process_flag(trap_exit, true),
                    {ok, Eof} = file:position(Fd, eof),

                    proc_lib:init_ack({ok, self()}),
                    InitState = #file{fd=Fd,
                                      eof=Eof,
                                      file_path=FilePath,
                                      open_options=OpenOptions},
                    gen_server:enter_loop(?MODULE, [], InitState);
                Error ->
                    proc_lib:init_ack(Error)
            end;
        Error ->
            proc_lib:init_ack(Error)
    end.

handle_call(close, _From, #file{fd=Fd}=File) ->
    {stop, normal, file:close(Fd), File#file{fd = nil}};

handle_call({pread_iolist, Pos}, _From, File) ->
    {RawData, NextPos} = try
        % up to 8Kbs of read ahead
        read_raw_iolist_int(File, Pos, 2 * ?SIZE_BLOCK - (Pos rem ?SIZE_BLOCK))
    catch
    _:_ ->
            read_raw_iolist_int(File, Pos, 4)
    end,
    {Begin, RestRawData} = split_iolist(RawData, 4, []),
    <<Prefix:1/integer, Len:31/integer>> = iolist_to_binary(Begin),
    case Prefix of
    1 ->
        {Crc32, IoList} = extract_crc32(
            maybe_read_more_iolist(RestRawData, 4 + Len, NextPos, File)),
        {reply, {ok, IoList, Crc32}, File};
    0 ->
        IoList = maybe_read_more_iolist(RestRawData, Len, NextPos, File),
        {reply, {ok, IoList, <<>>}, File}
    end;

handle_call(bytes, _From, #file{fd = Fd} = File) ->
    {reply, file:position(Fd, eof), File};

handle_call(sync, _From, #file{fd=Fd}=File) ->
    {reply, file:sync(Fd), File};

handle_call({truncate, Pos}, _From, #file{fd=Fd}=File) ->
    {ok, Pos} = file:position(Fd, Pos),
    case file:truncate(Fd) of
    ok ->
        {reply, ok, File#file{eof = Pos}};
    Error ->
        {reply, Error, File}
    end;

handle_call({set_path, NewFilePath}, _From, File) ->
    {reply, ok, File#file{file_path=NewFilePath}};

handle_call({rename, NewFilePath}, _From, #file{file_path=FilePath} = File) ->
    Reply = file:rename(FilePath, NewFilePath),
    {reply, Reply, File#file{file_path=NewFilePath}};

handle_call(reopen, _From, #file{fd=Fd, file_path=FilePath,
                                 open_options=OpenOptions}=File) ->

    ok = file:close(Fd),
    case try_open_fd(FilePath, OpenOptions, ?RETRY_TIME_MS,
                     ?MAX_RETRY_TIME_MS) of
        {ok, Fd2} ->
            {ok, Eof} = file:position(Fd, eof),
            {reply, ok, File#file{fd=Fd2, eof=Eof}};
        Error ->
            {stop, Error, File}
    end;

handle_call({append_bin, Bin}, _From, #file{fd = Fd, eof = Pos} = File) ->
    Blocks = make_blocks(Pos rem ?SIZE_BLOCK, Bin),
    Size = iolist_size(Blocks),
    case file:write(Fd, Blocks) of
    ok ->
        {reply, {ok, Pos, Size}, File#file{eof = Pos + Size}};
    Error ->
        {reply, Error, File}
    end;

handle_call({write_header, Bin}, _From, #file{fd = Fd, eof = Pos} = File) ->
    BinSize = byte_size(Bin),
    {Padding, Pos2} = case Pos rem ?SIZE_BLOCK of
    0 ->
        {<<>>, Pos};
    BlockOffset ->
        Pos1 = Pos + (?SIZE_BLOCK -  BlockOffset),
        {<<0:(8*(?SIZE_BLOCK-BlockOffset))>>, Pos1}
    end,
    FinalBin = [Padding, <<1, BinSize:32/integer>> | make_blocks(5, [Bin])],
    case file:write(Fd, FinalBin) of
    ok ->
        {reply, {ok, Pos2}, File#file{eof = Pos + iolist_size(FinalBin)}};
    Error ->
        {reply, Error, File}
    end;

handle_call(find_header, _From, #file{fd = Fd, eof = Pos} = File) ->
    {reply, find_header(Fd, Pos div ?SIZE_BLOCK), File}.

handle_cast(close, Fd) ->
    {stop,normal,Fd}.

handle_info({'EXIT', _, normal}, Fd) ->
    {noreply, Fd};
handle_info({'EXIT', _, Reason}, Fd) ->
    {stop, Reason, Fd}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #file{fd = nil}) ->
    ok;
terminate(_Reason, #file{fd = Fd}) ->
    ok = file:close(Fd).



maybe_create_file(FilePath, Options) ->
    IfCreate = case lists:member(create_if_missing, Options) of
        true ->
            case file:read_file_info(FilePath) of
                {error, enoent} -> true;
                _ -> lists:member(overwrite, Options)
            end;
        false ->
            lists:member(create, Options)
    end,

    case IfCreate of
        true ->
            filelib:ensure_dir(FilePath),
            case file:open(FilePath, [read, write, binary]) of
                {ok, Fd} ->
                    {ok, Length} = file:position(Fd, eof),
                    case Length > 0 of
                        true ->
                            % this means the file already exists and has data.
                            % FYI: We don't differentiate between empty files and non-existant
                            % files here.
                            case lists:member(overwrite, Options) of
                                true ->
                                    {ok, 0} = file:position(Fd, 0),
                                    ok = file:truncate(Fd),
                                    ok = file:sync(Fd),
                                    file:close(Fd);
                                false ->
                                    ok = file:close(Fd),
                                    file_exists
                            end;
                        false ->
                            file:close(Fd)
                    end;
                Error ->
                    Error
            end;
        false ->
            ok
    end.

try_open_fd(FilePath, Options, _Timewait, TotalTimeRemain)
        when TotalTimeRemain < 0 ->
    % Out of retry time.
    % Try one last time and whatever we get is the returned result.
    file:open(FilePath, Options);
try_open_fd(FilePath, Options, Timewait, TotalTimeRemain) ->
    case file:open(FilePath, Options) of
        {ok, Fd} ->
            {ok, Fd};
        {error, emfile} ->
            error_logger:info_msg("Too many file descriptors open, waiting"
                                  ++ " ~pms to retry", [Timewait]),
            receive
            after Timewait ->
                    try_open_fd(FilePath, Options, Timewait,
                                TotalTimeRemain - Timewait)
            end;
        {error, eacces} ->
            error_logger:info_msg("eacces error opening file ~p waiting"
                                  ++ " ~pms to retry", [FilePath, Timewait]),
            receive
            after Timewait ->
                    try_open_fd(FilePath, Options, Timewait,
                                TotalTimeRemain - Timewait)
            end;
        Error ->
            Error
    end.



find_header(_Fd, -1) ->
    no_valid_header;
find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
    {ok, Bin} ->
        {ok, Bin, Block * ?SIZE_BLOCK};
    _Error ->
        find_header(Fd, Block -1)
    end.

load_header(Fd, Block) ->
    {ok, <<1, HeaderLen:32/integer, RestBlock/binary>>} =
        file:pread(Fd, Block * ?SIZE_BLOCK, ?SIZE_BLOCK),
    TotalBytes = calculate_total_read_len(5, HeaderLen),
    RawBin = case TotalBytes > byte_size(RestBlock) of
        false ->
            <<RawBin1:TotalBytes/binary, _/binary>> = RestBlock,
            RawBin1;
        true ->
            {ok, Missing} = file:pread(
                    Fd, (Block * ?SIZE_BLOCK) + 5 + byte_size(RestBlock),
                    TotalBytes - byte_size(RestBlock)),
            <<RestBlock/binary, Missing/binary>>
    end,
    <<Crc32Sig:32/integer, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(RawBin, 5)),
    Crc32Sig = erlang:crc32(HeaderBin),
    {ok, HeaderBin}.


maybe_read_more_iolist(Buffer, DataSize, NextPos, Fd) ->
    case iolist_size(Buffer) of
        BufferSize when DataSize =< BufferSize ->
            {Buffer2, _} = split_iolist(Buffer, DataSize, []),
            Buffer2;
        BufferSize ->
            {Missing, _} = read_raw_iolist_int(Fd, NextPos, DataSize-BufferSize),
            [Buffer, Missing]
    end.

-spec read_raw_iolist_int(#file{}, Pos::non_neg_integer(), Len::non_neg_integer()) ->
    {Data::iolist(), CurPos::non_neg_integer()}.
read_raw_iolist_int(Fd, {Pos, _Size}, Len) -> % 0110 UPGRADE CODE
    read_raw_iolist_int(Fd, Pos, Len);
read_raw_iolist_int(#file{fd = Fd}, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_read_len(BlockOffset, Len),
    case file:pread(Fd, Pos, TotalBytes) of
        {ok, <<RawBin:TotalBytes/binary>>} ->
            {remove_block_prefixes(RawBin, BlockOffset), Pos + TotalBytes};
        {ok, RawBin} ->
            UnexpectedBin = {
                    unexpected_binary,
                    {at, Pos},
                    {wanted_bytes, TotalBytes},
                    {got, byte_size(RawBin), RawBin}
                    },
            throw({read_error, UnexpectedBin});
        Else ->
            throw({read_error, Else})
    end.

-spec extract_crc32(iolist()) -> {binary(), iolist()}.
extract_crc32(FullIoList) ->
    {CrcList, IoList} = split_iolist(FullIoList, 4, []),
    {iolist_to_binary(CrcList), IoList}.

calculate_total_read_len(0, FinalLen) ->
    calculate_total_read_len(1, FinalLen) + 1;
calculate_total_read_len(BlockOffset, FinalLen) ->
    case ?SIZE_BLOCK - BlockOffset of
    BlockLeft when BlockLeft >= FinalLen ->
        FinalLen;
    BlockLeft ->
        FinalLen + ((FinalLen - BlockLeft) div (?SIZE_BLOCK -1)) +
            if ((FinalLen - BlockLeft) rem (?SIZE_BLOCK -1)) =:= 0 -> 0;
                true -> 1 end
    end.



remove_block_prefixes(<<>>, _BlockOffset) ->
    [];
remove_block_prefixes(<<_BlockPrefix, Rest/binary>>, 0) ->
    remove_block_prefixes(Rest, 1);
remove_block_prefixes(Bin, BlockOffset) ->
    BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
    case size(Bin) of
    Size when Size > BlockBytesAvailable ->
        <<DataBlock:BlockBytesAvailable/binary,Rest/binary>> = Bin,
        [DataBlock | remove_block_prefixes(Rest, 0)];
    _Size ->
        [Bin]
    end.

make_blocks(_BlockOffset, []) ->
    [];
make_blocks(0, IoList) ->
    [<<0>> | make_blocks(1, IoList)];
make_blocks(BlockOffset, IoList) ->
    case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
    {Begin, End} ->
        [Begin | make_blocks(0, End)];
    _SplitRemaining ->
        IoList
    end.

%% @doc Returns a tuple where the first element contains the leading SplitAt
%% bytes of the original iolist, and the 2nd element is the tail. If SplitAt
%% is larger than byte_size(IoList), return the difference.
-spec split_iolist(IoList::iolist(), SplitAt::non_neg_integer(), Acc::list()) ->
    {iolist(), iolist()} | non_neg_integer().

split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([Bin | Rest], SplitAt, BeginAcc) when is_binary(Bin), SplitAt > byte_size(Bin) ->
    split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([Bin | Rest], SplitAt, BeginAcc) when is_binary(Bin) ->
    <<Begin:SplitAt/binary,End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRemaining ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).
