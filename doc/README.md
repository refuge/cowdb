

# CowDB - Pure Key/Value database for Erlang Applications #

Copyright (c) 2014 Benoit Chesneau and the contributors.

__Version:__ 0.4.1

__Authors:__ Benoit Chesneau ([`benoitc@refuge.io`](mailto:benoitc@refuge.io)).

## Description

CowDB implements an indexed, key/value storage engine.

## Features

- Append-Only b-tree using COW
- Read/Write can happen independently
- Put/Get/Delete/Fold operations support transactions (add, remove
operations on a 1 ore more store at once) transaction functions:
Transaction functions can atomically analyze and transform database
values in a transaction. You can use them to ensure atomic
read-modify-update processing, and integrity constraints.
- Transaction log
- Snapshots support: you are able to take a snapshot of the database
at any time (until the database is compacted)
- Destructive compaction to reclaim space in your database. The log
history is lost during the transaction.
- Automatic compaction

## Usage

Look at the [`cowdb`](cowdb.md) module for the API usage.

Full documentation is available here:
https://wiki.refuge.io/display/COWDB/CowDB+Documentation+Home

main CowDB website is http://cowdb.org

## Build process

### 1. Install rebar

To build CowDB you need to install rebar in your `PATH`. Rebar is
available on Github:

https://github.com/rebar/rebar

Follow the
[README](https://github.com/rebar/rebar/blob/master/README.md) to
install it.

### 2. Build the sources

Fetch the source code:

```
    $ git clone https://github.com/refuge/cowdb.git
```

Build the source, run the `make` command. It will fetch all required
dependencies.

```
    $ cd /<PATH_TO>/cowdb
    $ make
```

### 3. Run tests

```
    $ make test
```

## Example of usage:

```
    1> {ok, Pid} = cowdb:open("testing.db").
    {ok,<0.35.0>}
    2> cowdb:put(Pid, a, 1).
    {ok, 1}
    3> cowdb:get(Pid, a).
    {ok,{a,1}}
    4> cowdb:mget(Pid, [a, b]).
    [{ok,{a,1}},not_found]
    5> cowdb:put(Pid, b, 2).
    {ok, 2}
    6> cowdb:mget(Pid, [a, b]).
    [{ok,{a,1}},{ok,{b,2}}]
    7> cowdb:mget(Pid, [a, b, c, d]).
    [{ok,{a,1}},{ok,{b,2}},not_found,not_found]
    8> cowdb:transact(Pid, [
        {add, c, 2},
        {remove, b},
        {fn, fun(Db) ->
                    {ok, {a, V}} = cowdb:get(Db, a),
                    [{add, d, V}] end}]).
    {ok, 3}
    9> cowdb:mget(Pid, [a, b, c, d]).
    [{ok,{a,1}},not_found,{ok,{c,2}},{ok,{d,1}}]
    10> cowdb:fold(Pid, fun(Got, Acc) -> {ok, [Got | Acc]} end, []).
    {ok,[{d,1},{c,2},{a,1}]}
```

## Ownership and License

The contributors are listed in AUTHORS. This project uses the MPL v2
license, see LICENSE.

rbeacon uses the [C4.1 (Collective Code Construction
Contract)](http://rfc.zeromq.org/spec:22) process for contributions.

## Development

Under C4.1 process, you are more than welcome to help us by:

* join the discussion over anything from design to code style try out
* and [submit issue reports](https://github.com/refuge/cowdb/issues/new)
* or feature requests pick a task in
* [issues](https://github.com/refuge/cowdb/issues) and get it done fork
* the repository and have your own fixes send us pull requests and even
* star this project ^_^

To  run the test suite:

```
    $ make test
```



## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="cowdb.md" class="module">cowdb</a></td></tr>
<tr><td><a href="cowdb_btree.md" class="module">cowdb_btree</a></td></tr>
<tr><td><a href="cowdb_btree_copy.md" class="module">cowdb_btree_copy</a></td></tr>
<tr><td><a href="cowdb_compress.md" class="module">cowdb_compress</a></td></tr>
<tr><td><a href="cowdb_file.md" class="module">cowdb_file</a></td></tr>
<tr><td><a href="cowdb_util.md" class="module">cowdb_util</a></td></tr></table>

