

# Module cowdb_compress #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-compression_method">compression_method()</a> ###



<pre><code>
compression_method() = snappy | lz4 | gzip | none
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#compress-2">compress/2</a></td><td>compress an encoded binary with the following type.</td></tr><tr><td valign="top"><a href="#decompress-1">decompress/1</a></td><td>decompress a binary to an erlang decoded term.</td></tr><tr><td valign="top"><a href="#is_compressed-2">is_compressed/2</a></td><td>check if the binary has been compressed.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="compress-2"></a>

### compress/2 ###


<pre><code>
compress(Bin::binary() | term(), Method::<a href="#type-compression_method">compression_method()</a>) -&gt; Bin::binary()
</code></pre>
<br />

compress an encoded binary with the following type. When an
erlang term is given it is encoded to a binary.
<a name="decompress-1"></a>

### decompress/1 ###


<pre><code>
decompress(Bin::binary()) -&gt; Term::term()
</code></pre>
<br />

decompress a binary to an erlang decoded term.
<a name="is_compressed-2"></a>

### is_compressed/2 ###


<pre><code>
is_compressed(Bin::binary() | term(), Method::<a href="#type-compression_method">compression_method()</a>) -&gt; true | false
</code></pre>
<br />

check if the binary has been compressed.
