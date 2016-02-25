# AirLock

Preparation repository for submissions to Curiosity 

Only database module is being develop for now.

## Database Docs

[Design](https://github.com/PiotrekDlang/AirLock/blob/master/docs/database/design.md)

## Roadmap
### Version 0.1 (2016-05-01)
* Migrate to GitLab (GitHub and Bitbucket is excluded because of its active politics against Christianity)
* Better support for complex types (arrays, composition, etc.)
* Cell/Page allocator
* Physical file management
* Better documentation
    * file format: header, table, cell, etc.
    * SW design : components' definitions
* examples of corresponding SQL syntax
* more unit testing

### Future Development
* Indexes (for lookup speed-up)
* Proper memory management (no gc)
* Locking and multi threading
* Transactions
* Journaling and write ahead logging
* Recovery
* Optimization (db size and execution time)