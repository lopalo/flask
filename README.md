# flask
Key-value storage server with LSM-tree for storing data sets larger than the amount of RAM.
The storage implements basic key-value operations and provides range queries for a keyspace.

The server makes use of request multiplexing over a single TCP connection to avoid
head-of-line blocking, meaning queries that access "hot" data (working set) are handled first.
That implies that the protocol might reorder the responses arbitrarily.

