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

-define(DEFAULT_COMPRESSION, snappy).

-define(MIN_STR, <<"">>).
-define(MAX_STR, <<255>>). % illegal utf string


-define(b2l(V), binary_to_list(V)).
-define(l2b(V), list_to_binary(V)).
-define(i2b(V), couch_util:integer_to_boolean(V)).
-define(b2i(V), couch_util:boolean_to_integer(V)).
-define(term_to_bin(T), term_to_binary(T, [{minor_version, 1}])).
-define(term_size(T),
    try
        erlang:external_size(T)
    catch _:_ ->
        byte_size(?term_to_bin(T))
    end).


-define(LATEST_DISK_VERSION, 6).

-record(db_header,
    {disk_version = ?LATEST_DISK_VERSION,
     update_seq = 0,
     unused = 0,
     id_tree_state = nil,
     seq_tree_state = nil,
     local_tree_state = nil,
     purge_seq = 0,
     purged_docs = nil,
     security_ptr = nil,
     revs_limit = 1000
    }).

-record(db,
    {main_pid = nil,
    compactor_pid = nil,
    instance_start_time, % number of microsecs since jan 1 1970 as a binary string
    fd,
    fd_monitor,
    header = #db_header{},
    committed_update_seq,
    id_tree,
    seq_tree,
    local_tree,
    update_seq,
    name,
    filepath,
    validate_doc_funs = [],
    security = [],
    security_ptr = nil,
    user_ctx = nil,
    waiting_delayed_commit = nil,
    revs_limit = 1000,
    fsync_options = [],
    options = [],
    compression,
    before_doc_update = nil, % nil | fun(Doc, Db) -> NewDoc
    after_doc_read = nil,     % nil | fun(Doc, Db) -> NewDoc
    after_db_update = nil %  nil | fun(Db) -> Db,
    }).


-record(reduce_fold_helper_funs, {
    start_response,
    send_row
}).

-record(btree, {
    fd,
    root,
    extract_kv,
    assemble_kv,
    less,
    reduce = nil,
    compression = ?DEFAULT_COMPRESSION,
    chunk_threshold = 16#4ff
}).

-record(proc, {
    pid,
    lang,
    client = nil,
    ddoc_keys = [],
    prompt_fun,
    prompt_many_fun,
    set_timeout_fun,
    stop_fun
}).

-record(leaf,  {
    deleted,
    ptr,
    seq,
    size = nil
}).


-record(httpd,
    {mochi_req,
    peer,
    method,
    requested_path_parts,
    path_parts,
    db_url_handlers,
    user_ctx,
    req_body = undefined,
    design_url_handlers,
    auth,
    default_fun,
    url_handlers
    }).
