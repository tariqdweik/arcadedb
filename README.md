# arcadedb
Super Fast Multi-Model DBMS

## Differences between OrientDB and ArcadeDB

## What Arcade does not support

- ArcadeDB does not suport *inheritance* (but it's in the roadmap)
- ArcadeDB does not handle object reference, so if you modify an object, other instances couldn't be updated and a reload is necessary. This was to avoid to handle a local cache of POJOs (as a Map<RID,Object> in transaction context)
- ArcadeDB doesn't support update when the record get larger than the stored version (but it's in the roadmap)
- ArcadeDB doesn't storing support records with a size major than the page size. You can always create a bucket with a larger page size, but this can be done only at creation time
- ArcadeDB works only in embedded mode, there is 

## What Arcade has more than OrientDB

- ArcadeDB is much Faster
- ArcadeDB uses much less RAM
- ArcadeDB allows to execute operation in asynchronously way (by using `.async()`).
