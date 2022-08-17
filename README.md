# Okay WAL

A [write-ahead log (WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging)
implementation for Rust.

> There's The Great Wall, and then there's this: an okay WAL.

This crate exposes a WAL that supports:

- ACID-compliant writes from multiple threads.
- Random access for previously written data.
- Supports automatic archiving/checkpointing to allow reusing disk space and
  preventing the WAL from growing too large.
- Interactive recovery process with basic data versioning support.

## Multi-threaded characteristics

This WAL implementation does not attempt to guarantee that recovery of WAL
entries is performed in the same order in which the writes were written to the
log. For example, imagine two threads writing to the log simultaneously. Each
write to the log requires writing the actual data and then calling commit, which
issues a file synchronization call to ensure all bytes are fully flushed to
disk.

What is the order in which the two entries should be recovered in? If the WAL is
being written to while modifying an in-memory data structure, the actual order
of the changes is being determined outside of the order of the bytes being
stored within the WAL. This means that the WAL has no way to properly know the
exact order of the data.

During recovery, `okaywal` will recover entries in the order in which they were
started. This may require you to either store additional data in the log or
handle out-of-order operations manually.

For single-threaded writes, the order is guaranteed to be the order the data is
written in.

[wal]: https://en.wikipedia.org/wiki/Write-ahead_logging
