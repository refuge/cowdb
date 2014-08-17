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
-module(cowdb_compaction).

-include("cowdb.hrl").

-export([compact_path/1,
         delete_compact_file/1,
         start/2,
         cancel/1]).

compact_path(#db{file_path=FilePath}) ->
    FilePath ++ "-compact".

delete_compact_file(Db) ->
    CompactFile = compact_path(Db),
    RootDir = filename:dirname(CompactFile),
    catch cowdb_file:delete(RootDir, CompactFile).


start(#db{updater_pid=UpdaterPid}=Db, Options) ->
    CompactFile = compact_path(Db),

    NewDb = case file:read_file_info(CompactFile) of
        {ok, _} ->
            {ok, TargetDb} = make_target_db(Db, CompactFile),
            do_compact(Db, TargetDb, true);
        {error, enoent} ->
            %% initial compaction
            {ok, TargetDb} = make_target_db(Db, CompactFile),
            do_compact(Db, TargetDb, false)
    end,
    close_db(NewDb),
    case cowdb_updater:req(UpdaterPid, {compact_done, CompactFile}) of
        ok -> ok;
        {retry, CurrentDb} ->
            start(CurrentDb, Options)
    end.


cancel(#db{compactor_info=nil}=Db) ->
    Db;
cancel(#db{compactor_info=Pid}=Db) ->
    cowdb_util:shutdown_sync(Pid),
    delete_compact_file(Db),
    Db#db{compactor_info=nil}.

%% do initial compaction, copy the id btree.
do_compact(#db{tid=LastTid, by_id=IdBt, reader_fd=ReaderFd},
           #db{fd=Fd, log=LogBt}=TargetDb, false) ->
    %% copy the IDs btree to the new database
    CopyFun = fun({K, {_, {Pos, _}, Tid, Ts}}, Acc) ->
                   %% copy the value
                   {ok, Val} = cowdb_file:pread_term(ReaderFd, Pos),
                   {ok, NewPos, Size} = cowdb_file:append_term_crc32(Fd, Val),

                   {{K, {K, {NewPos, Size}, Tid, Ts}}, Acc}
    end,
    {ok, IdRoot, _} = cowdb_btree_copy:copy(IdBt#btree{fd=ReaderFd}, Fd,
                                          [{before_kv_write, {CopyFun, nil}}]),
    IdBt2 = IdBt#btree{fd=Fd, root=IdRoot},

    %% store a new transaction that point to this btree
    Transaction = {LastTid, #transaction{tid=LastTid,
                                         by_id=IdBt2,
                                         ops=[],
                                         ts =
                                         cowdb_util:timestamp()}},
    {ok, LogBt2} = cowdb_btree:add(LogBt, [Transaction]),
    %% finally commit the result to the file.
    TargetDb1 = TargetDb#db{by_id=IdBt2, log=LogBt2},
    {ok, TargetDb2} = cowdb_util:commit_transaction(LastTid, TargetDb1),
    TargetDb2;
%% retry the compaction, in that case we are using the log to replay the
%% transactions. We only add and update keys/values, folding from recent
%% to old.
do_compact(#db{tid=LastTid, log=LogBt0, reader_fd=ReaderFd},
           #db{tid=Tid0, fd=Fd, by_id=IdBt, log=LogBt1}=TargetDb,
           _Retry) ->

    CopyFun = fun({_TransactId, #transaction{ops=Ops}}, {IdBt1, Handled}) ->
            {ToAdd, ToRem, Handled2} =  copy_from_log(Ops, LastTid, ReaderFd,
                                                      Fd, [], [], Handled),
            {ok, IdBt2} = cowdb_btree:add_remove(IdBt1, ToAdd, ToRem),
            {ok, {IdBt2, Handled2}}
    end,

    {ok, _, {FinalIdBt, _}} = cowdb_btree:fold(LogBt0#btree{fd=ReaderFd},
                                             CopyFun, {IdBt, []},
                                             [rev, {start_key, LastTid},
                                              {end_key, Tid0}]),

    %% store a new transaction that point to this btree
    Transaction = {LastTid, #transaction{tid=LastTid,
                                         by_id=cowdb_btree:get_state(FinalIdBt),
                                         ops=[],
                                         ts =
                                         cowdb_util:timestamp()}},
    {ok, LogBt2} = cowdb_btree:add(LogBt1, [Transaction]),

    %% finally commit the result to the file.
    TargetDb1 = TargetDb#db{by_id=FinalIdBt, log=LogBt2},
    {ok, TargetDb2} = cowdb_util:commit_transaction(LastTid, TargetDb1),
    TargetDb2.


copy_from_log([], _TransactId, _ReaderFd, _Fd, ToAdd, ToRem, Handled) ->
    {lists:reverse(ToAdd), lists:reverse(ToRem), Handled};
copy_from_log([{Op, {Key, {Pos, _}, _, Ts}} | Rest], TransactId, ReaderFd,
              Fd, ToAdd, ToRem, Handled) ->
    case lists:member(Key, Handled) of
        true ->
            copy_from_log(Rest, TransactId, ReaderFd, Fd, ToAdd, ToRem,
                          Handled);
        false ->
            case Op of
                add ->
                    {ok, Val} = cowdb_file:pread_term(ReaderFd, Pos),
                    {ok, NewPos, Size} = cowdb_file:append_term_crc32(Fd, Val),
                    Add = {Key, {Key, {NewPos, Size}, TransactId, Ts}},
                    copy_from_log(Rest, TransactId, ReaderFd, Fd,
                                  [Add | ToAdd], ToRem, [Key | Handled]);
                remove ->
                    copy_from_log(Rest, TransactId, ReaderFd, Fd, ToAdd,
                                  [Key |ToRem], [Key | Handled])
            end
    end.

close_db(#db{fd=Fd}) ->
    cowdb_file:close(Fd).

init_db(Db, CompactFile, Header, Fd) ->
    NewDb = cowdb_util:init_db(Header, Db#db.db_pid, Fd, Fd, CompactFile,
                                Db#db.options),
    unlink(Fd),
    NewDb.

make_target_db(#db{tid=Tid}=Db, CompactFile) ->
    case cowdb_file:open(CompactFile) of
        {ok, Fd} ->
            case cowdb_file:read_header(Fd) of
                {ok, Header, _Pos} ->
                    {ok, init_db(Db, CompactFile, Header, Fd)};
                no_valid_header ->
                    {error, no_valid_header}
            end;
        {error, enoent} ->
            {ok, Fd} = cowdb_file:open(CompactFile, [create]),
            Header = #db_header{tid=Tid},
            {ok, _Pos} = cowdb_file:write_header(Fd, Header),
            {ok, init_db(Db, CompactFile, Header, Fd)}
    end.
