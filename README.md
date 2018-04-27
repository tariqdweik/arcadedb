# arcadedb
Super Fast Multi-Model DBMS

## Differences between OrientDB and ArcadeDB

## What Arcade does not support

- ArcadeDB doesn't support update when the record get larger than the stored version (but it's in the roadmap)
- ArcadeDB doesn't support storing records with a size major than the page size. You can always create a bucket with a larger page size, but this can be done only at creation time
- ArcadeDB cannot be replicated distributed
- ArcadeDB remote server supports only HTTP/JSON, no binary protocol is available
- ArcadeDB doesn't provide a dirty manager, so it's up to the developer to mark the object to save by calling `.save()` method. This makes the code of ArcadeDB smaller without handling edge cases

## What Arcade has more than OrientDB

- ArcadeDB is much Faster
- ArcadeDB uses much less RAM
- ArcadeDB allows to execute operation in asynchronously way (by using `.async()`).
- ArcadeDB is only a few KBs
