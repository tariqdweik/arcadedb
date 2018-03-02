ROADMAP

v0.1
- support for records > 65K size by using multiple pages
- implement update
- when the db is open, set db.lock file to catch soft shutdown
- in case of non-soft shutdown, check CRC of the most recent touched pages
- replace corrupted pages from WAL files

v0.4
- delete writes on a separate file that keep all the holes (use bitset, 1 bit per page)
- 1st implementation of approx index
- when an insert operation check last page has no much left room, suggest flushing

0.9
- deferred delete (at page flushing). This will speed up deletion


