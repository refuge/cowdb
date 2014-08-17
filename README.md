#cowdb

CowDB implements an indexed, key/value storage engine. The primary index
is an append-only btree which is implemented by CBT - a btree library
extracted from Apache CouchDB .

CowDB is released under the Mozilla Public License, v. 2.0

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


## Documentation

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

    $ git clone https://bitbucket.org/refugeio/cowdb.git

Build the source, run the `make` command. It will fetch all required
dependencies.

    $ cd /<PATH_TO>/cowdb
    $ make


### 3. Build the docs

    $ make doc

and open the `index.html` file in the doc directory. Or read it
[online](http://refugeio.bitbucket.org/cowdb/index.html).

### 4. Run tests

    $ make test

## Example of usage:

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

## Contribute

Open Issues and Support tickets in [Jira](https://issues.refuge.io/browse/CDB).
Code is available on [bitbucket](https://bitbucket.org/refugeio/cowdb).
