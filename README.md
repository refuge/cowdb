#cbt

multi-layer MVCC log append-only database based on the Apache CouchDB btree.

## build

### 1. install rebar
To build cbt you need to install rebar in your `PATH`. Rebar is
available on Github:

https://github.com/rebar/rebar

Follow the
[README](https://github.com/rebar/rebar/blob/master/README.md) to
install it.

### 2. build

Fetch the source code:

    $ git clone git@bitbucket.org:refugeio/cbt.git

Build the source, run the `make` command. It will fetch any needed
dependencies.

    $ cd /<PATH_TO>/cbt
    $ make

### 3. test CBT

Run the following command line:

    $ make test


### 3. Build the doc

    $ make doc

and open the `index.html` file in the doc folder.
