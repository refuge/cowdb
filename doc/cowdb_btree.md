

# Module cowdb_btree #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-btree">btree()</a> ###



<pre><code>
btree() = #btree{}
</code></pre>





### <a name="type-btree_fold_options">btree_fold_options()</a> ###



<pre><code>
btree_fold_options() = [{dir, fwd | rev} | {start_key, term()} | {end_key, term()} | {end_key_gt, term()} | {key_group_fun, function()}]
</code></pre>





### <a name="type-btree_keys">btree_keys()</a> ###



<pre><code>
btree_keys() = [term()]
</code></pre>





### <a name="type-btree_kv">btree_kv()</a> ###



<pre><code>
btree_kv() = {Key::any(), Val::any()}
</code></pre>





### <a name="type-btree_kvs">btree_kvs()</a> ###



<pre><code>
btree_kvs() = [<a href="#type-btree_kv">btree_kv()</a>]
</code></pre>





### <a name="type-btree_options">btree_options()</a> ###



<pre><code>
btree_options() = [{split, function()} | {join, function()} | {less, function()} | {reduce, function()} | {compression, <a href="cowdb_compress.md#type-compression_method">cowdb_compress:compression_method()</a>} | {kv_chunk_threshold, integer()} | {kp_chunk_threshold, integer()}]
</code></pre>





### <a name="type-btree_root">btree_root()</a> ###



<pre><code>
btree_root() = {integer(), list(), integer()}
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add-2">add/2</a></td><td>insert a list of key/values in the btree.</td></tr><tr><td valign="top"><a href="#add_remove-3">add_remove/3</a></td><td>insert and remove a list of key/values in the btree in one
write.</td></tr><tr><td valign="top"><a href="#final_reduce-2">final_reduce/2</a></td><td>apply the reduce function on last reductions.</td></tr><tr><td valign="top"><a href="#fold-3">fold/3</a></td><td>fold key/values in the btree.</td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td></td></tr><tr><td valign="top"><a href="#fold_reduce-4">fold_reduce/4</a></td><td>fold reduce values.</td></tr><tr><td valign="top"><a href="#full_reduce-1">full_reduce/1</a></td><td>return the full reduceed value from the btree.</td></tr><tr><td valign="top"><a href="#get_state-1">get_state/1</a></td><td>return the latest btree root that will be stored in the database
header or value.</td></tr><tr><td valign="top"><a href="#less-3">less/3</a></td><td></td></tr><tr><td valign="top"><a href="#lookup-2">lookup/2</a></td><td>lookup for a list of keys in the btree
Results are returned in the same order as the keys.</td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td>open a btree from the file.</td></tr><tr><td valign="top"><a href="#open-3">open/3</a></td><td>open a btree from the file.</td></tr><tr><td valign="top"><a href="#query_modify-4">query_modify/4</a></td><td>insert and remove a list of key/values and retrieve a list of
key/values from their key in the btree in one call.</td></tr><tr><td valign="top"><a href="#set_options-2">set_options/2</a></td><td>set btreee options.</td></tr><tr><td valign="top"><a href="#size-1">size/1</a></td><td>return the size in bytes of a btree.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add-2"></a>

### add/2 ###


<pre><code>
add(Btree::<a href="#type-btree">btree()</a>, InsertKeyValues::<a href="#type-btree_kvs">btree_kvs()</a>) -&gt; {ok, Btree2::<a href="#type-btree">btree()</a>}
</code></pre>
<br />

insert a list of key/values in the btree
<a name="add_remove-3"></a>

### add_remove/3 ###


<pre><code>
add_remove(Btree::<a href="#type-btree">btree()</a>, InsertKeyValues::<a href="#type-btree_kvs">btree_kvs()</a>, RemoveKeys::<a href="#type-btree_keys">btree_keys()</a>) -&gt; {ok, Btree2::<a href="#type-btree">btree()</a>}
</code></pre>
<br />

insert and remove a list of key/values in the btree in one
write.
<a name="final_reduce-2"></a>

### final_reduce/2 ###


<pre><code>
final_reduce(Btree::<a href="#type-btree">btree()</a>, LastReduction::{any(), any()}) -&gt; term()
</code></pre>
<br />

apply the reduce function on last reductions.
<a name="fold-3"></a>

### fold/3 ###


<pre><code>
fold(Btree::<a href="#type-btree">btree()</a>, Fun::function(), Acc::term()) -&gt; {ok, {KVs::<a href="#type-btree_kvs">btree_kvs()</a>, Reductions::[term()]}, Acc2::term()}
</code></pre>
<br />

fold key/values in the btree
<a name="fold-4"></a>

### fold/4 ###


<pre><code>
fold(Btree::<a href="#type-btree">btree()</a>, Fun::function(), Acc::term(), Options::<a href="#type-btree_fold_options">btree_fold_options()</a>) -&gt; {ok, {KVs::<a href="#type-btree_kvs">btree_kvs()</a>, Reductions::[term()]}, Acc2::term()}
</code></pre>
<br />


<a name="fold_reduce-4"></a>

### fold_reduce/4 ###


<pre><code>
fold_reduce(Btree::<a href="#type-btree">btree()</a>, FoldFun::function(), Acc::any(), Options::<a href="#type-btree_fold_options">btree_fold_options()</a>) -&gt; {ok, Acc2::term()}
</code></pre>
<br />

fold reduce values.

<a name="full_reduce-1"></a>

### full_reduce/1 ###


<pre><code>
full_reduce(Btree::<a href="#type-btree">btree()</a>) -&gt; {ok, term()}
</code></pre>
<br />

return the full reduceed value from the btree.
<a name="get_state-1"></a>

### get_state/1 ###


<pre><code>
get_state(Btree::<a href="#type-btree">btree()</a>) -&gt; State::tuple()
</code></pre>
<br />

return the latest btree root that will be stored in the database
header or value
<a name="less-3"></a>

### less/3 ###

`less(Btree, A, B) -> any()`


<a name="lookup-2"></a>

### lookup/2 ###


<pre><code>
lookup(Btree::<a href="#type-btree">btree()</a>, Keys::<a href="#type-btree_keys">btree_keys()</a>) -&gt; [{ok, <a href="#type-btree_kv">btree_kv()</a>} | not_found]
</code></pre>
<br />

lookup for a list of keys in the btree
Results are returned in the same order as the keys. If the key is
not_found the `not_found` result is appended to the list.
<a name="open-2"></a>

### open/2 ###


<pre><code>
open(State::nil | <a href="#type-btree">btree()</a>, Fd::<a href="cowdb_file.md#type-cowdb_file">cowdb_file:cowdb_file()</a>) -&gt; {ok, <a href="#type-btree">btree()</a>}
</code></pre>
<br />

open a btree from the file.
pass in 'nil' for State if a new Btree.
<a name="open-3"></a>

### open/3 ###


<pre><code>
open(State::nil | <a href="#type-btree">btree()</a>, Fd::<a href="cowdb_file.md#type-cowdb_file">cowdb_file:cowdb_file()</a>, Options::<a href="#type-btree_options">btree_options()</a>) -&gt; {ok, <a href="#type-btree">btree()</a>}
</code></pre>
<br />

open a btree from the file.
pass in 'nil' for State if a new Btree.
Options:

* {split, fun(Btree, Value)} : Take a value and extract content if
needed from it. It returns a {key, Value} tuple. You don't need to
set such function if you already give a {Key, Value} tuple to your
add/add_remove functions.

* {join, fun(Key, Value)} : The fonction takes the key and value and
return a new Value ussed when you lookup. By default it return a
{Key, Value} .

* {reduce_fun, ReduceFun} : pass the reduce fun

* {compression, nonde | snappy}: the compression methods used to
compress the data

* {less, LessFun(KeyA, KeyB)}: function used to order the btree that
compare two keys


<a name="query_modify-4"></a>

### query_modify/4 ###


<pre><code>
query_modify(Btree::<a href="#type-btree">btree()</a>, LookupKeys::<a href="#type-btree_keys">btree_keys()</a>, InsertKeyValues::<a href="#type-btree_kvs">btree_kvs()</a>, RemoveKeys::<a href="#type-btree_keys">btree_keys()</a>) -&gt; {ok, FoundKeyValues::<a href="#type-btree_kvs">btree_kvs()</a>, Btree2::<a href="#type-btree">btree()</a>}
</code></pre>
<br />

insert and remove a list of key/values and retrieve a list of
key/values from their key in the btree in one call.
<a name="set_options-2"></a>

### set_options/2 ###


<pre><code>
set_options(Btree::<a href="#type-btree">btree()</a>, Options::<a href="#type-btree_options">btree_options()</a>) -&gt; Btree2::<a href="#type-btree">btree()</a>
</code></pre>
<br />

set btreee options
<a name="size-1"></a>

### size/1 ###


<pre><code>
size(Btree::<a href="#type-btree">btree()</a>) -&gt; Size::integer()
</code></pre>
<br />

return the size in bytes of a btree
