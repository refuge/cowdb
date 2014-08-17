

# Module cowdb_btree_copy #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-btree_copy_options">btree_copy_options()</a> ###



<pre><code>
btree_copy_options() = [{before_kv_write, {function(), any()}} | {filter, function()} | override | {compression, <a href="cowdb_compress.md#type-compression_method">cowdb_compress:compression_method()</a>}]
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#copy-3">copy/3</a></td><td>copy a btree to a cbt file process.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="copy-3"></a>

### copy/3 ###


<pre><code>
copy(Btree::<a href="cowdb_btree.md#type-btree">cowdb_btree:btree()</a>, Fd::<a href="cowdb_file.md#type-cowdb_file">cowdb_file:cowdb_file()</a>, Options::<a href="#type-btree_copy_options">btree_copy_options()</a>) -&gt; {ok, <a href="cowdb_btree.md#type-btree_root">cowdb_btree:btree_root()</a>, any()}
</code></pre>
<br />

copy a btree to a cbt file process.
Options are:

* `{before_kv_write, fun(Item, Acc) -> {Newttem, NewAcc} end, InitAcc}`:
to edit a Key/Vlaue before it's written to the new btree.

* `{filter, fun(Item) -> true | false end}`, function to filter the
items copied to the new btree.

* `override`: if the new file should be truncated

* `{compression, Module}`: to change the compression on the new
btree.


