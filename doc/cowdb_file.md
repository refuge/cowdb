

# Module cowdb_file #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

__Behaviours:__ [`gen_server`](gen_server.md).

<a name="types"></a>

## Data Types ##




### <a name="type-append_options">append_options()</a> ###



<pre><code>
append_options() = [{compression, <a href="cowdb_compress.md#type-compression_method">cowdb_compress:compression_method()</a>}]
</code></pre>





### <a name="type-cowdb_file">cowdb_file()</a> ###



<pre><code>
cowdb_file() = pid()
</code></pre>





### <a name="type-file_option">file_option()</a> ###



<pre><code>
file_option() = create | overwrite
</code></pre>





### <a name="type-file_options">file_options()</a> ###



<pre><code>
file_options() = [<a href="#type-file_option">file_option()</a>]
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#append_binary-2">append_binary/2</a></td><td>append an Erlang binary to the end of the file.</td></tr><tr><td valign="top"><a href="#append_binary_crc32-2">append_binary_crc32/2</a></td><td>append an Erlang binary to the end of the file and sign in with
crc32.</td></tr><tr><td valign="top"><a href="#append_raw_chunk-2">append_raw_chunk/2</a></td><td>like append_binary but wihout manipulating the binary, it is
stored as is.</td></tr><tr><td valign="top"><a href="#append_term-2">append_term/2</a></td><td>append an Erlang term to the end of the file.</td></tr><tr><td valign="top"><a href="#append_term-3">append_term/3</a></td><td></td></tr><tr><td valign="top"><a href="#append_term_crc32-2">append_term_crc32/2</a></td><td>append an Erlang term to the end of the file and sign with an
crc32 prefix.</td></tr><tr><td valign="top"><a href="#append_term_crc32-3">append_term_crc32/3</a></td><td></td></tr><tr><td valign="top"><a href="#assemble_file_chunk-1">assemble_file_chunk/1</a></td><td></td></tr><tr><td valign="top"><a href="#assemble_file_chunk-2">assemble_file_chunk/2</a></td><td></td></tr><tr><td valign="top"><a href="#bytes-1">bytes/1</a></td><td>get he length of a file, in bytes.</td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td>Close the file.</td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td>delete a file synchronously.</td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td>delete a file asynchronously or not.</td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#init_delete_dir-1">init_delete_dir/1</a></td><td>utility function to init the deletion directory where the
deleted files will be temporarely stored.</td></tr><tr><td valign="top"><a href="#nuke_dir-2">nuke_dir/2</a></td><td>utility function to remove completely the content of a directory.</td></tr><tr><td valign="top"><a href="#open-1">open/1</a></td><td>open a file in a gen_server that will be used to handle btree
I/Os.</td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#pread_binary-2">pread_binary/2</a></td><td> Reads a binrary from a file that was written with append_binary
Args:    Pos, the offset into the file where the term is serialized.</td></tr><tr><td valign="top"><a href="#pread_iolist-2">pread_iolist/2</a></td><td></td></tr><tr><td valign="top"><a href="#pread_term-2">pread_term/2</a></td><td>Reads a term from a file that was written with append_term
Args:    Pos, the offset into the file where the term is serialized.</td></tr><tr><td valign="top"><a href="#read_header-1">read_header/1</a></td><td>read the database header from the database file.</td></tr><tr><td valign="top"><a href="#rename-2">rename/2</a></td><td>rename a file safely.</td></tr><tr><td valign="top"><a href="#reopen-1">reopen/1</a></td><td>reopen a file.</td></tr><tr><td valign="top"><a href="#sync-1">sync/1</a></td><td>Ensure all bytes written to the file are flushed to disk.</td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr><tr><td valign="top"><a href="#truncate-2">truncate/2</a></td><td>Truncate a file to the number of bytes.</td></tr><tr><td valign="top"><a href="#write_header-2">write_header/2</a></td><td>write the database header at the end of the the database file.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="append_binary-2"></a>

### append_binary/2 ###


<pre><code>
append_binary(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Bin::binary()) -&gt; {ok, Pos::integer(), NumBytesWriiten::integer} | {error, term}
</code></pre>
<br />

append an Erlang binary to the end of the file.
Args:    Erlang term to serialize and append to the file.
Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to the
beginning the serialized term. Use pread_term to read the term back.
or {error, Reason}.
<a name="append_binary_crc32-2"></a>

### append_binary_crc32/2 ###


<pre><code>
append_binary_crc32(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Bin::binary()) -&gt; {ok, Pos::integer(), NumBytesWriiten::integer} | {error, term}
</code></pre>
<br />

append an Erlang binary to the end of the file and sign in with
crc32.
<a name="append_raw_chunk-2"></a>

### append_raw_chunk/2 ###


<pre><code>
append_raw_chunk(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Bin::binary()) -&gt; {ok, Pos::integer(), NumBytesWriiten::integer} | {error, term}
</code></pre>
<br />

like append_binary but wihout manipulating the binary, it is
stored as is.
<a name="append_term-2"></a>

### append_term/2 ###


<pre><code>
append_term(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Term::term()) -&gt; {ok, Pos::integer(), NumBytesWriiten::integer} | {error, term}
</code></pre>
<br />

append an Erlang term to the end of the file.
Args:    Erlang term to serialize and append to the file.
Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to
the beginning the serialized  term. Use pread_term to read the term
back.
or {error, Reason}.
<a name="append_term-3"></a>

### append_term/3 ###


<pre><code>
append_term(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Term::term(), Options::<a href="#type-append_options">append_options()</a>) -&gt; {ok, Pos::integer(), NumBytesWriiten::integer} | {error, term}
</code></pre>
<br />


<a name="append_term_crc32-2"></a>

### append_term_crc32/2 ###


<pre><code>
append_term_crc32(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Term::term()) -&gt; {ok, Pos::integer(), NumBytesWriiten::integer} | {error, term}
</code></pre>
<br />

append an Erlang term to the end of the file and sign with an
crc32 prefix.
<a name="append_term_crc32-3"></a>

### append_term_crc32/3 ###


<pre><code>
append_term_crc32(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Term::term(), Options::<a href="#type-append_options">append_options()</a>) -&gt; {ok, Pos::integer(), NumBytesWriiten::integer} | {error, term}
</code></pre>
<br />


<a name="assemble_file_chunk-1"></a>

### assemble_file_chunk/1 ###

`assemble_file_chunk(Bin) -> any()`


<a name="assemble_file_chunk-2"></a>

### assemble_file_chunk/2 ###

`assemble_file_chunk(Bin, Crc32) -> any()`


<a name="bytes-1"></a>

### bytes/1 ###


<pre><code>
bytes(Fd::<a href="#type-cowdb_file">cowdb_file()</a>) -&gt; {ok, Bytes::integer()} | {error, term()}
</code></pre>
<br />

get he length of a file, in bytes.
<a name="close-1"></a>

### close/1 ###


<pre><code>
close(Fd::<a href="#type-cowdb_file">cowdb_file()</a>) -&gt; ok
</code></pre>
<br />

Close the file.
<a name="code_change-3"></a>

### code_change/3 ###

`code_change(OldVsn, State, Extra) -> any()`


<a name="delete-2"></a>

### delete/2 ###


<pre><code>
delete(RootDir::string(), FilePath::string()) -&gt; ok | {error, term()}
</code></pre>
<br />

delete a file synchronously.
Root dir is the root where to find the file. This call is blocking
until the file is deleted.
<a name="delete-3"></a>

### delete/3 ###


<pre><code>
delete(RootDir::string(), FilePath::string(), Async::boolean()) -&gt; ok | {error, term()}
</code></pre>
<br />

delete a file asynchronously or not
<a name="handle_call-3"></a>

### handle_call/3 ###

`handle_call(X1, From, File) -> any()`


<a name="handle_cast-2"></a>

### handle_cast/2 ###

`handle_cast(X1, Fd) -> any()`


<a name="handle_info-2"></a>

### handle_info/2 ###

`handle_info(X1, Fd) -> any()`


<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`


<a name="init_delete_dir-1"></a>

### init_delete_dir/1 ###


<pre><code>
init_delete_dir(RootDir::string()) -&gt; ok
</code></pre>
<br />

utility function to init the deletion directory where the
deleted files will be temporarely stored.
<a name="nuke_dir-2"></a>

### nuke_dir/2 ###


<pre><code>
nuke_dir(RootDelDir::string(), Dir::string()) -&gt; ok
</code></pre>
<br />

utility function to remove completely the content of a directory
<a name="open-1"></a>

### open/1 ###


<pre><code>
open(FilePath::string()) -&gt; {ok, <a href="#type-cowdb_file">cowdb_file()</a>} | {error, term()}
</code></pre>
<br />

open a file in a gen_server that will be used to handle btree
I/Os.
<a name="open-2"></a>

### open/2 ###


<pre><code>
open(FilePath::string(), Options::<a href="#type-file_options">file_options()</a>) -&gt; {ok, <a href="#type-cowdb_file">cowdb_file()</a>} | {error, term()}
</code></pre>
<br />


<a name="pread_binary-2"></a>

### pread_binary/2 ###


<pre><code>
pread_binary(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Pos::integer()) -&gt; {ok, Bin::binary()} | {error, term()}
</code></pre>
<br />

 Reads a binrary from a file that was written with append_binary
Args:    Pos, the offset into the file where the term is serialized.
<a name="pread_iolist-2"></a>

### pread_iolist/2 ###

`pread_iolist(Fd, Pos) -> any()`


<a name="pread_term-2"></a>

### pread_term/2 ###


<pre><code>
pread_term(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Pos::integer()) -&gt; {ok, Term::term()} | {error, term()}
</code></pre>
<br />

Reads a term from a file that was written with append_term
Args:    Pos, the offset into the file where the term is serialized.
<a name="read_header-1"></a>

### read_header/1 ###


<pre><code>
read_header(Fd::<a href="#type-cowdb_file">cowdb_file()</a>) -&gt; {ok, Header::term(), Pos::integer()} | {error, term()}
</code></pre>
<br />

read the database header from the database file
<a name="rename-2"></a>

### rename/2 ###


<pre><code>
rename(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, NewFilePath::string()) -&gt; ok | {error, term()}
</code></pre>
<br />

rename a file safely
<a name="reopen-1"></a>

### reopen/1 ###


<pre><code>
reopen(Fd::<a href="#type-cowdb_file">cowdb_file()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

reopen a file
<a name="sync-1"></a>

### sync/1 ###


<pre><code>
sync(FdOrPath::<a href="#type-cowdb_file">cowdb_file()</a> | string()) -&gt; ok | {error, term()}
</code></pre>
<br />

Ensure all bytes written to the file are flushed to disk.
<a name="terminate-2"></a>

### terminate/2 ###

`terminate(Reason, File) -> any()`


<a name="truncate-2"></a>

### truncate/2 ###


<pre><code>
truncate(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Pos::integer()) -&gt; ok | {error, term()}
</code></pre>
<br />

Truncate a file to the number of bytes.
<a name="write_header-2"></a>

### write_header/2 ###


<pre><code>
write_header(Fd::<a href="#type-cowdb_file">cowdb_file()</a>, Header::term()) -&gt; {ok, Pos::integer()} | {error, term()}
</code></pre>
<br />

write the database header at the end of the the database file
