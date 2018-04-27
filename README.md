# arcadedb
Super Fast Multi-Model DBMS

## Differences between OrientDB and ArcadeDB

- ArcadeDB shares the same database instance across threads. Much easier developing code with ArcadeDB than OrientDB with multi-threads applications
- ArcadeDB uses thread locals only to manage transactions, while OrientDB makes a strong usage of TL internally
- The OrientDB classes are Types in ArcadeDB
- There is no base V and E classes in ArcadeDB, but vertex and edge are types of records
- ArcadeDB saves every type and property name in the dictionary to compress the record by storing only the names ids
- ArcadeDB keeps the MVCC counter on the page rather than on the record
- ArcadeDB manage everything as files and pages

## What Arcade does not support

- ArcadeDB doesn't support update when the record get larger than the stored version (but it's in the roadmap)
- ArcadeDB doesn't support storing records with a size major than the page size. You can always create a bucket with a larger page size, but this can be done only at creation time
- ArcadeDB cannot be replicated distributed
- ArcadeDB remote server supports only HTTP/JSON, no binary protocol is available
- ArcadeDB doesn't provide a dirty manager, so it's up to the developer to mark the object to save by calling `.save()` method. This makes the code of ArcadeDB smaller without handling edge cases

## What Arcade has more than OrientDB

- ArcadeDB saves every type and property name in the dictionary to compress the record by storing only the names ids
- ArcadeDB asynchronous API automatically balance the load on the available cores
- ArcadeDB is much Faster
- ArcadeDB uses much less RAM
- ArcadeDB allows to execute operation in asynchronously way (by using `.async()`).
- ArcadeDB is only a few KBs
