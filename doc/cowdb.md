

# Module cowdb #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

__Behaviours:__ [`gen_server`](gen_server.md).

<a name="types"></a>

## Data Types ##




### <a name="type-compression_method">compression_method()</a> ###



<pre><code>
compression_method() = snappy | lz4 | gzip | {deflate, Level::integer()} | none
</code></pre>





### <a name="type-cow_mfa">cow_mfa()</a> ###



<pre><code>
cow_mfa() = {local, Name::atom()} | {global, GlobalName::term()} | {via, ViaName::term()}
</code></pre>





### <a name="type-db">db()</a> ###



<pre><code>
db() = #db{} | pid()
</code></pre>





### <a name="type-fold_options">fold_options()</a> ###



<pre><code>
fold_options() = [{dir, fwd | rev} | {start_key, term()} | {end_key, term()} | {end_key_gt, term()} | {key_group_fun, function()}]
</code></pre>





### <a name="type-fsync_options">fsync_options()</a> ###



<pre><code>
fsync_options() = [before_header | after_header | on_file_open]
</code></pre>





### <a name="type-open_options">open_options()</a> ###



<pre><code>
open_options() = [{compression, <a href="#type-compression_method">compression_method()</a>} | {fsync_options, <a href="#type-fsync_options">fsync_options()</a>} | auto_compact | {auto_compact, boolean()} | {compact_limit, integer()} | {reduce, function()} | {less, function()} | {init_func, function()}]
</code></pre>





### <a name="type-transact_fn">transact_fn()</a> ###



<pre><code>
transact_fn() = {module(), function(), [any()]} | {module(), function()} | function()
</code></pre>





### <a name="type-transact_id">transact_id()</a> ###



<pre><code>
transact_id() = integer() | tx_end
</code></pre>





### <a name="type-transact_ops">transact_ops()</a> ###



<pre><code>
transact_ops() = [{add, term(), any()} | {remove, term()} | {fn, <a href="#type-transact_fn">transact_fn()</a>}]
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#cancel_compact-1">cancel_compact/1</a></td><td>cancel compaction.</td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td>Close the file.</td></tr><tr><td valign="top"><a href="#compact-1">compact/1</a></td><td>compact the database file.</td></tr><tr><td valign="top"><a href="#count-1">count/1</a></td><td>get the number of objects stored in the database.</td></tr><tr><td valign="top"><a href="#data_size-1">data_size/1</a></td><td>get the total size of the objects stored in the database.</td></tr><tr><td valign="top"><a href="#database_info-1">database_info/1</a></td><td>returns database info.</td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td>delete one object from the store.</td></tr><tr><td valign="top"><a href="#drop_db-1">drop_db/1</a></td><td>delete a database.</td></tr><tr><td valign="top"><a href="#drop_db-2">drop_db/2</a></td><td>delete a database asynchronously or not.</td></tr><tr><td valign="top"><a href="#fold-3">fold/3</a></td><td>fold all objects form the database.</td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td>fold all objects form the database with range options.</td></tr><tr><td valign="top"><a href="#fold_reduce-4">fold_reduce/4</a></td><td>fold the reduce function over the results.</td></tr><tr><td valign="top"><a href="#full_reduce-1">full_reduce/1</a></td><td>return the full reduced value.</td></tr><tr><td valign="top"><a href="#get-2">get/2</a></td><td>get an object by the specified key.</td></tr><tr><td valign="top"><a href="#get_snapshot-2">get_snapshot/2</a></td><td>get a snapshot of the database at some point.</td></tr><tr><td valign="top"><a href="#log-4">log/4</a></td><td>fold the transaction log.</td></tr><tr><td valign="top"><a href="#log-5">log/5</a></td><td>fold the transaction log
Args:
<ul>
<li><code>Db</code>: the db value (in transaction function) or pid</li>
<li><code>StartT</code>: transaction ID to start from</li>
<li><code>EndT</code>: transaction ID to stop</li>
<li><code>Fun</code>: function collection log result:
<pre>  fun({TransactId, Op, {K,V}, Ts}, Acc) ->
       {ok, Acc2} | {stop, Acc2}
   end</pre>
where TransactId is the transaction ID <code>Transactid</code> where the <code>OP</code>
(<code>add</code> or <code>remove</code>) on the Key/Value pair <code>{K, V}</code> has been run on
the unix time <code>Ts</code>.</li>
<li><code>Acc</code>: initial value to pass to the function.</li>
</ul>
The function return the total number of transactions in the range and
the values collected during folding.</td></tr><tr><td valign="top"><a href="#lookup-2">lookup/2</a></td><td>deprecated: use mget/2 instead.</td></tr><tr><td valign="top"><a href="#mdelete-2">mdelete/2</a></td><td>delete multiple object at once.</td></tr><tr><td valign="top"><a href="#mget-2">mget/2</a></td><td>get a list of objects by the specified key.</td></tr><tr><td valign="top"><a href="#mput-2">mput/2</a></td><td>add multiple objects to a store.</td></tr><tr><td valign="top"><a href="#open-1">open/1</a></td><td>open a cowdb database, pass a function to initialise the stores and
indexes.</td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td>open a cowdb database, pass a function to initialise the stores and
indexes.</td></tr><tr><td valign="top"><a href="#open-3">open/3</a></td><td>Create or open a cowdb store with a registered name.</td></tr><tr><td valign="top"><a href="#open_link-1">open_link/1</a></td><td>open a cowdb database as part of the supervision tree, pass a
function to initialise the stores and indexes.</td></tr><tr><td valign="top"><a href="#open_link-2">open_link/2</a></td><td>open a cowdb database as part of the supervision tree.</td></tr><tr><td valign="top"><a href="#open_link-3">open_link/3</a></td><td>open a cowdb database as part of the supervision tree with a
registered name.</td></tr><tr><td valign="top"><a href="#put-2">put/2</a></td><td>add one object to a store.</td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td>add one object to a store.</td></tr><tr><td valign="top"><a href="#transact-2">transact/2</a></td><td>execute a transaction
A transaction received operations to execute as a list:
<ul>
<li><code>{add, Key, Value}</code> to add an object</li>
<li><code>{remove, Key}</code> to remove a value</li>
<li> <code>{fn, Func}</code> a transaction function. A transaction function
received the db value like it was at the beginning of the transaction
as an argument. It's possible to pass arguments to it. A transaction
function return a list of operations and can query/manipulate
function. The list of operations returned can also contain a
function.</li>
</ul>.</td></tr><tr><td valign="top"><a href="#transact-3">transact/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="cancel_compact-1"></a>

### cancel_compact/1 ###


<pre><code>
cancel_compact(Ref::<a href="#type-db">db()</a>) -&gt; ok
</code></pre>
<br />

cancel compaction
<a name="close-1"></a>

### close/1 ###


<pre><code>
close(DbPid::pid()) -&gt; ok
</code></pre>
<br />

Close the file.
<a name="compact-1"></a>

### compact/1 ###


<pre><code>
compact(Ref::<a href="#type-db">db()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

compact the database file
<a name="count-1"></a>

### count/1 ###


<pre><code>
count(DbPid::<a href="#type-db">db()</a>) -&gt; {ok, integer()} | {error, term()}
</code></pre>
<br />

get the number of objects stored in the database.
<a name="data_size-1"></a>

### data_size/1 ###


<pre><code>
data_size(DbPid::<a href="#type-db">db()</a>) -&gt; {ok, integer()} | {error, term()}
</code></pre>
<br />

get the total size of the objects stored in the database.
<a name="database_info-1"></a>

### database_info/1 ###


<pre><code>
database_info(DbPid::<a href="#type-db">db()</a>) -&gt; {ok, list()}
</code></pre>
<br />

returns database info
<a name="delete-2"></a>

### delete/2 ###


<pre><code>
delete(Db::<a href="#type-db">db()</a>, Key::term()) -&gt; {ok, <a href="#type-transact_id">transact_id()</a>} | {error, term()}
</code></pre>
<br />

delete one object from the store
<a name="drop_db-1"></a>

### drop_db/1 ###


<pre><code>
drop_db(DbPid::<a href="#type-db">db()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

delete a database
<a name="drop_db-2"></a>

### drop_db/2 ###


<pre><code>
drop_db(DbPid::<a href="#type-db">db()</a>, Async::boolean()) -&gt; ok | {error, term()}
</code></pre>
<br />

delete a database asynchronously or not
<a name="fold-3"></a>

### fold/3 ###


<pre><code>
fold(DbPid::<a href="#type-db">db()</a>, Fun::function(), Acc::any()) -&gt; {ok, any(), any()} | {error, term()}
</code></pre>
<br />

fold all objects form the database
<a name="fold-4"></a>

### fold/4 ###


<pre><code>
fold(DbPid::<a href="#type-db">db()</a>, Fun::function(), Acc::any(), Options::<a href="#type-fold_options">fold_options()</a>) -&gt; {ok, any()} | {error, term()}
</code></pre>
<br />

fold all objects form the database with range options
<a name="fold_reduce-4"></a>

### fold_reduce/4 ###

`fold_reduce(DbPid, Fun, Acc, Options) -> any()`

fold the reduce function over the results.
<a name="full_reduce-1"></a>

### full_reduce/1 ###


<pre><code>
full_reduce(DbPid::<a href="#type-db">db()</a>) -&gt; {ok, any()}
</code></pre>
<br />

return the full reduced value
<a name="get-2"></a>

### get/2 ###


<pre><code>
get(Db::<a href="#type-db">db()</a>, Key::any()) -&gt; {ok, any()} | {error, term()}
</code></pre>
<br />

get an object by the specified key
<a name="get_snapshot-2"></a>

### get_snapshot/2 ###


<pre><code>
get_snapshot(DbPid::<a href="#type-db">db()</a>, TransactId::<a href="#type-transact_id">transact_id()</a>) -&gt; {ok, <a href="#type-db">db()</a>} | {error, term()}
</code></pre>
<br />

get a snapshot of the database at some point.
<a name="log-4"></a>

### log/4 ###


<pre><code>
log(Db::<a href="#type-db">db()</a>, StartT::<a href="#type-transact_id">transact_id()</a>, Function::function(), Acc::any()) -&gt; {ok, NbTransactions::integer(), Acc2::any()} | {error, term()}
</code></pre>
<br />

fold the transaction log
<a name="log-5"></a>

### log/5 ###


<pre><code>
log(Db::<a href="#type-db">db()</a>, StartT::<a href="#type-transact_id">transact_id()</a>, EndT::<a href="#type-transact_id">transact_id()</a>, Function::function(), Acc::any()) -&gt; {ok, NbTransactions::integer(), Acc2::any()} | {error, term()}
</code></pre>
<br />

fold the transaction log
Args:

* `Db`: the db value (in transaction function) or pid

* `StartT`: transaction ID to start from

* `EndT`: transaction ID to stop

* `Fun`: function collection log result:

```
  fun({TransactId, Op, {K,V}, Ts}, Acc) ->
       {ok, Acc2} | {stop, Acc2}
   end
```

where TransactId is the transaction ID `Transactid` where the `OP`
(`add` or `remove`) on the Key/Value pair `{K, V}` has been run on
the unix time `Ts`.

* `Acc`: initial value to pass to the function.


The function return the total number of transactions in the range and
the values collected during folding.
<a name="lookup-2"></a>

### lookup/2 ###


<pre><code>
lookup(Db::<a href="#type-db">db()</a>, Keys::[any()]) -&gt; {ok, any()} | {error, term()}
</code></pre>
<br />

deprecated: use mget/2 instead.
<a name="mdelete-2"></a>

### mdelete/2 ###


<pre><code>
mdelete(Db::<a href="#type-db">db()</a>, Keys::[term()]) -&gt; {ok, <a href="#type-transact_id">transact_id()</a>} | {error, term()}
</code></pre>
<br />

delete multiple object at once
<a name="mget-2"></a>

### mget/2 ###


<pre><code>
mget(Db::<a href="#type-db">db()</a>, Keys::[any()]) -&gt; {ok, any()} | {error, term()}
</code></pre>
<br />

get a list of objects by the specified key
<a name="mput-2"></a>

### mput/2 ###


<pre><code>
mput(Db::<a href="#type-db">db()</a>, KVs::[{term(), any()}]) -&gt; {ok, <a href="#type-transact_id">transact_id()</a>} | {error, term()}
</code></pre>
<br />

add multiple objects to a store
<a name="open-1"></a>

### open/1 ###


<pre><code>
open(FilePath::string()) -&gt; {ok, Db::pid()} | {error, term()}
</code></pre>
<br />

open a cowdb database, pass a function to initialise the stores and
indexes.
<a name="open-2"></a>

### open/2 ###


<pre><code>
open(FilePath::string(), Option::<a href="#type-open_options">open_options()</a>) -&gt; {ok, Db::pid()} | {error, term()}
</code></pre>
<br />

open a cowdb database, pass a function to initialise the stores and
indexes.
<a name="open-3"></a>

### open/3 ###


<pre><code>
open(Name::<a href="#type-cow_mfa">cow_mfa()</a>, FilePath::string(), Option::<a href="#type-open_options">open_options()</a>) -&gt; {ok, Db::pid()} | {error, term()}
</code></pre>
<br />

Create or open a cowdb store with a registered name.
<a name="open_link-1"></a>

### open_link/1 ###


<pre><code>
open_link(FilePath::string()) -&gt; {ok, Db::pid()} | {error, term()}
</code></pre>
<br />

open a cowdb database as part of the supervision tree, pass a
function to initialise the stores and indexes.
<a name="open_link-2"></a>

### open_link/2 ###


<pre><code>
open_link(FilePath::string(), Option::<a href="#type-open_options">open_options()</a>) -&gt; {ok, Db::pid()} | {error, term()}
</code></pre>
<br />

open a cowdb database as part of the supervision tree
<a name="open_link-3"></a>

### open_link/3 ###


<pre><code>
open_link(Name::<a href="#type-cow_mfa">cow_mfa()</a>, FilePath::string(), Option::<a href="#type-open_options">open_options()</a>) -&gt; {ok, Db::pid()} | {error, term()}
</code></pre>
<br />

open a cowdb database as part of the supervision tree with a
registered name
<a name="put-2"></a>

### put/2 ###


<pre><code>
put(DbPid::<a href="#type-db">db()</a>, X2::{term(), any()}) -&gt; {ok, <a href="#type-transact_id">transact_id()</a>} | {error, term()}
</code></pre>
<br />

add one object to a store
<a name="put-3"></a>

### put/3 ###


<pre><code>
put(DbPid::<a href="#type-db">db()</a>, Key::term(), Value::any()) -&gt; {ok, <a href="#type-transact_id">transact_id()</a>} | {error, term()}
</code></pre>
<br />

add one object to a store
<a name="transact-2"></a>

### transact/2 ###


<pre><code>
transact(Ref::<a href="#type-db">db()</a>, OPs::<a href="#type-transact_ops">transact_ops()</a>) -&gt; {ok, <a href="#type-transact_id">transact_id()</a>} | {error, term()}
</code></pre>
<br />

execute a transaction
A transaction received operations to execute as a list:

* `{add, Key, Value}` to add an object

* `{remove, Key}` to remove a value

* `{fn, Func}` a transaction function. A transaction function
received the db value like it was at the beginning of the transaction
as an argument. It's possible to pass arguments to it. A transaction
function return a list of operations and can query/manipulate
function. The list of operations returned can also contain a
function.



<a name="transact-3"></a>

### transact/3 ###


<pre><code>
transact(Ref::<a href="#type-db">db()</a>, OPs::<a href="#type-transact_ops">transact_ops()</a>, Timeout::timeout()) -&gt; {ok, <a href="#type-transact_id">transact_id()</a>} | {error, term()}
</code></pre>
<br />


