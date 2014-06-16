

# Module cowdb_updater #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-trans_type">trans_type()</a> ###



<pre><code>
trans_type() = version_change | update
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#call-3">call/3</a></td><td></td></tr><tr><td valign="top"><a href="#call-4">call/4</a></td><td></td></tr><tr><td valign="top"><a href="#cancel_compact-1">cancel_compact/1</a></td><td></td></tr><tr><td valign="top"><a href="#compact-2">compact/2</a></td><td></td></tr><tr><td valign="top"><a href="#get_db-1">get_db/1</a></td><td>get latest db state.</td></tr><tr><td valign="top"><a href="#init-5">init/5</a></td><td></td></tr><tr><td valign="top"><a href="#init_db-6">init_db/6</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-5">start_link/5</a></td><td></td></tr><tr><td valign="top"><a href="#transact-3">transact/3</a></td><td>send a transaction and wait for its result.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="call-3"></a>

### call/3 ###

`call(UpdaterPid, Label, Args) -> any()`


<a name="call-4"></a>

### call/4 ###

`call(UpdaterPid, Label, Args, Timeout) -> any()`


<a name="cancel_compact-1"></a>

### cancel_compact/1 ###

`cancel_compact(UpdaterPid) -> any()`


<a name="compact-2"></a>

### compact/2 ###

`compact(UpdaterPid, Options) -> any()`


<a name="get_db-1"></a>

### get_db/1 ###


<pre><code>
get_db(Pid::pid()) -&gt; <a href="cowdb.md#type-db">cowdb:db()</a>
</code></pre>
<br />

get latest db state.
<a name="init-5"></a>

### init/5 ###

`init(DbPid, Fd, ReaderFd, FilePath, Options) -> any()`


<a name="init_db-6"></a>

### init_db/6 ###

`init_db(Header, DbPid, Fd, ReaderFd, FilePath, Options) -> any()`


<a name="start_link-5"></a>

### start_link/5 ###

`start_link(DbPid, Fd, ReaderFd, FilePath, Options) -> any()`


<a name="transact-3"></a>

### transact/3 ###

`transact(UpdaterPid, Ops, Timeout) -> any()`

send a transaction and wait for its result.
