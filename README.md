# dechib

A simple database implementation in order to learn more about databases. **DO
NOT USE THIS IN PRODUCTION!**. Instead use it maybe as a reference and
hopefully it can be of some help.

Currently only the basic table storage and data insertions are supported.
Queries and other things on WIP!

## Components

A database is compromised of a number of components.

### The API

A database isn't useful without a way to use it. Database APIs are often 
connection-based, stateful and sequential. For a single client connected the
queries need to be executed in sequence, we want all our operations to be
tied together and we want to be able to mutate the database, and also do
things like rollback operations.

I should implement an implementation of both sides of this but I'll look at
existing APIs in order to guide my thinking.

### Query Parsing

Here we parse the queries and turn them into something to execute. To make
this simpler since parsers aren't that interesting to me I'm just going to
use [sqlparser](https://crates.io/crates/sqlparser) and take the AST it
outputs and then just pass that onto the next stage which is....

### Query Optimiser 

Something very interesting to me, this optimises the query and attempts to
create a query plan to execute which should hopefully run quickly. This is
an NP hard problem so there's a bunch of heuristics and other such fun goodies!

### Query Executor

Executes the query, this does things like interface with the storage engine to
ensure things receive the locks they need (transactions etc), and apply the
operations to the stored representation of the database. Once again something I
aim to implement myself.

### Storage Engine

Storing data on disk and dealing with all the pain and suffering that is
filesystems. Here to attempt to keep things simple I'm using
[rocksdb](https://crates.io/crates/rocksdb)
