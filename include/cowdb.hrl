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

-define(SERVER_NAME, cowdb).

-define(DEFAULT_COMPRESSION, none).

-define(term_to_bin(T), term_to_binary(T, [{minor_version, 1}])).
-define(term_size(T),
    try
        erlang:external_size(T)
    catch _:_ ->
        byte_size(?term_to_bin(T))
    end).




-record(btree, {fd,
                root,
                extract_kv = identity,  % fun({_Key, _Value} = KV) -> KV end,,
                assemble_kv =  identity, % fun({Key, Value}) -> {Key, Value} end,
                less = fun(A, B) -> A < B end,
                reduce = nil,
                compression = ?DEFAULT_COMPRESSION,
                kv_chunk_threshold =  16#4ff,
                kp_chunk_threshold = 2 * 16#4ff}).

-define(DISK_VERSION, 1).

-define(COWDB_CALL(Pid, Req), {'$cowdb_call', Pid, Req}).

-record(db_header, {version=?DISK_VERSION,
                    tid=-1,
                    by_id=nil,
                    log=nil}).

-record(db, {parent,
             server,
             fd,
             name,
             mode,
             file,
             update_mode}).


-record(open_args, {file,
                    mode,
                    wal_thresold}).

-record(transaction, {tid,
                      by_id=nil,
                      ops=[],
                      ts}).
