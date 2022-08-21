# OkayWAL

A [write-ahead log (WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging)
implementation for Rust.

> There's The Great Wall, and then there's this: an okay WAL.

**WARNING: This crate is early in development. Please do not use in any projects
until this has been incorporated into
[Sediment](https://github.com/khonsulabs/sediment) and shipping as part of
[Nebari](https://github.com/khonsulabs/nebari).**

![okaywal forbids unsafe code](https://img.shields.io/badge/unsafe-forbid-success)
[![crate version](https://img.shields.io/crates/v/okaywal.svg)](https://crates.io/crates/okaywal)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/okaywal/Tests/main)](https://github.com/khonsulabs/okaywal/actions?query=workflow:Tests)
[![HTML Coverage Report for `main` branch](https://khonsulabs.github.io/okaywal/coverage/badge.svg)](https://khonsulabs.github.io/okaywal/coverage/)
[![Documentation](https://img.shields.io/badge/docs-main-informational)](https://khonsulabs.github.io/okaywal/main/okaywal)

This crate exposes a WAL that supports:

- Atomic and Durable writes from multiple threads.
- Random access for previously written data.
- Automatic checkpointing to allow reusing disk space and
  preventing the WAL from growing too large.
- Interactive recovery process with basic data versioning support.

## Basic How-To

[`WriteAheadLog::recover()`](https://khonsulabs.github.io/okaywal/main/okaywal/struct.WriteAheadLog.html#method.recover) is used to create or recover a WAL
in a given directory. To open a log, an implementator of
[`Checkpointer`](https://khonsulabs.github.io/okaywal/main/okaywal/trait.Checkpointer.html) must be provided. This trait is how
OkayWAL communicates with your code when recovering or checkpointing a log.

The [basic example][basic-example] shows this process with many comments
describing how OkayWAL works.

```rust,ignore
// Open a log using an Checkpointer that echoes the information passed into each
// function that the Checkpointer trait defines.
let log = WriteAheadLog::recover("my-log", LoggingCheckpointer)?;

// Begin writing an entry to the log.
let mut writer = log.write()?;

// Each entry is one or more chunks of data. Each chunk can be individually
// addressed using its LogPosition.
let record = writer.write_chunk("this is the first entry".as_bytes())?;

// To fully flush all written bytes to disk and make the new entry
// resilliant to a crash, the writer must be committed.
writer.commit()?;
```

## Multi-threaded Ordering of Entries

This WAL implementation has a simple approach to ordering entries: the order in
which `WriteAheadLog::write` is called controls the order in which the entries
are read. When only one thread is writing to the WAL at any given time, this
order is expected and predictable.

However, when multiple threads are writing to the WAL at the same time, there is
a range of time in which the entry is started, written, and then synchronized to
disk. If these threads are operating on an in-memory state while also writing to
the WAL, there is opportunity for information stored in the WAL to be out of
order relative to the operations being performed in-memory.

OkayWAL does not provide any facilities for helping sort out these
inconsistencies. As such, it may be important to store extra information inside
of the log entries to help ensure the exact state can be reproduced from the log
entries despite these inconsistencies.

[wal]: https://en.wikipedia.org/wiki/Write-ahead_logging
[basic-example]: https://github.com/khonsulabs/okaywal/blob/main/examples/basic.rs

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
