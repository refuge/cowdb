

# Module cowdb_util #
* [Function Index](#index)
* [Function Details](#functions)


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#apply-2">apply/2</a></td><td></td></tr><tr><td valign="top"><a href="#commit_transaction-2">commit_transaction/2</a></td><td>commit the transction on the disk.</td></tr><tr><td valign="top"><a href="#delete_file-1">delete_file/1</a></td><td>delete a file safely.</td></tr><tr><td valign="top"><a href="#delete_file-2">delete_file/2</a></td><td></td></tr><tr><td valign="top"><a href="#delete_property-2">delete_property/2</a></td><td></td></tr><tr><td valign="top"><a href="#init_db-6">init_db/6</a></td><td>initialise the db.</td></tr><tr><td valign="top"><a href="#maybe_sync-3">maybe_sync/3</a></td><td>test if the db file should be synchronized or not depending on the
state.</td></tr><tr><td valign="top"><a href="#set_property-3">set_property/3</a></td><td></td></tr><tr><td valign="top"><a href="#shutdown_sync-1">shutdown_sync/1</a></td><td>synchronous shutdown.</td></tr><tr><td valign="top"><a href="#timestamp-0">timestamp/0</a></td><td></td></tr><tr><td valign="top"><a href="#write_header-2">write_header/2</a></td><td>write the db header.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="apply-2"></a>

### apply/2 ###

`apply(Func, Args) -> any()`


<a name="commit_transaction-2"></a>

### commit_transaction/2 ###

`commit_transaction(TransactId, Db) -> any()`

commit the transction on the disk.
<a name="delete_file-1"></a>

### delete_file/1 ###

`delete_file(FilePath) -> any()`

delete a file safely
<a name="delete_file-2"></a>

### delete_file/2 ###

`delete_file(FilePath, Async) -> any()`


<a name="delete_property-2"></a>

### delete_property/2 ###

`delete_property(Key, Props) -> any()`


<a name="init_db-6"></a>

### init_db/6 ###

`init_db(Header, DbPid, Fd, ReaderFd, FilePath, Options) -> any()`

initialise the db
<a name="maybe_sync-3"></a>

### maybe_sync/3 ###

`maybe_sync(Status, Fd, FSyncOptions) -> any()`

test if the db file should be synchronized or not depending on the
state
<a name="set_property-3"></a>

### set_property/3 ###

`set_property(Key, Value, Props) -> any()`


<a name="shutdown_sync-1"></a>

### shutdown_sync/1 ###

`shutdown_sync(Pid) -> any()`

synchronous shutdown
<a name="timestamp-0"></a>

### timestamp/0 ###

`timestamp() -> any()`


<a name="write_header-2"></a>

### write_header/2 ###

`write_header(Header, Db) -> any()`

write the db header
