## Status: Proof of concept

For the time being, don't use for other purposes than fun and development

## Architecture

The D database engine design is inspired by SQLite database. 
Only some high level concepts are similar. In general, all details differ greatly.

![Architecture](arch.png)

## Key features (unfinished)

* ACID
* fast
* full compatibility with D algorithms and ranges
* nothrow @safe @nogc
* minimal storage size in one file