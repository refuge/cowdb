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


-define(DISK_VERSION, 1).

-record(db_header, {version=?DISK_VERSION,
                    db_version=1,
                    root=nil}).

-record(db, {version,
             db_pid,
             updater_pid,
             fd,
             reader_fd,
             root=nil,
             stores= [],
             old_stores=[],
             db_mod,
             header,
             file_path,
             fsync_options}).

-record(store, {db,
                id}).

-define(IF_TRANS(Status, Fun),
        case erlang:get(cowdb_trans) of
            Status -> Fun();
            Other -> {bad_transaction_state, Other}
        end).
