#cowdb

object database in pure Erlang.

## Features

- based on the Apache CouchDB btree
- multiple store support
- support transactions (add, remove operations on a 1 ore more store at once)
- transaction function support: cowdbwill invoke database functions as part of transaction processing. Functions written for this purpose are called transaction functions.

### Transaction functions:

A transaction function must expect to be passed a database value as its first argument.
This is to allow transaction function to issue queries etc. Other args can be given to it.

Additionally, a transaction function must return transaction operations. (other functions can be part of it).


## build

### 1. install rebar
To build cowdb you need to install rebar in your `PATH`. Rebar is
available on Github:

https://github.com/rebar/rebar

Follow the
[README](https://github.com/rebar/rebar/blob/master/README.md) to
install it.

### 2. build

Fetch the source code:

    $ git clone git@bitbucket.org:refugeio/cowdb.git

Build the source, run the `make` command. It will fetch any needed
dependencies.

    $ cd /<PATH_TO>/cowdb
    $ make

### 3. test cowdb

Run the following command line:

    $ make test


### 3. Build the doc

    $ make doc

and open the `index.html` file in the doc folder. Or read it
[online](http://refugeio.bitbucket.org/cowdb/index.html).


Example of usage:

        
    1> {ok, Pid} = cowdb:open("testing.db",
            1> fun(St, Db) -> cowdb:open_store(Db, "test") end).
    {ok,<0.35.0>}
    2> cowdb:lookup(Pid, "test", [a,b]).
    [{ok,{a,1}},{ok,{b,2}}]
    3> cowdb:lookup(Pid, "test", [a,b,c]).
    [{ok,{a,1}},{ok,{b,2}},not_found]
    4> cowdb:transact(Pid, [{remove, "test", b}, {add, "test", {c,
            3}}]).
    ok
    5> cowdb:lookup(Pid, "test", [a,b,c]).
    [{ok,{a,1}},not_found,{ok,{c,3}}]
    6> cowdb:get(Pid, "test", a).
    {ok,{a,1}}
    7> cowdb:transact(Pid, [{fn, fun(Db) -> [{add, "test", {d, 2}}]
            end}]).
    ok
    8> cowdb:lookup(Pid, "test", [d]).
    [{ok,{d,2}}]

## contribute

Open Issues and Support tickets in [Jira](https://issues.refuge.io/browse/COWDB
).
Code is available on [bitbucket](https://bitbucket.org/refugeio/cowdb).