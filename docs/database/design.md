## About

This is a project of a database engine embedded into the D language. It does't use any other technologies like SQL. It can be seen as a variant of object database. Queries are constructed by D syntax (e.g. using  std.algorithm) mainly via range interface. 

## Status: Proof of concept

For the time being, don't use for other purposes than fun and development

## Architecture

The D database engine design is inspired by SQLite database backend. 
Only some high level concepts are similar. In general, all details differ greatly.
Because there is no translation to SQL or any other intermediate layer, a query execution is managed directly by user code. 

![Architecture](arch.png)

## Key features (unfinished)

* ACID
* SQL is not use at any stage
* fast
* full compatibility with D algorithms and ranges
* nothrow @safe @nogc
* minimal storage size in one file

## Disadvantages
* The RPC nature of SQL queries are not possible without additional functionality and currently there are no plans to allow it.