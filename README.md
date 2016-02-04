# AirLock
Preparation repository for submissions to Curiosity 

## Docs

[Design](https://github.com/PiotrekDlang/AirLock/blob/master/docs/database/design.md)

## Roadmap
### Version 0.1 (2016-05-01)
* Array as object member 
* Page allocation 
* Physical file management
* Full object composition
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