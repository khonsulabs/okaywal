# OkayWAL

A [write-ahead log (WAL)][wal] implementation for Rust.

> There's The Great Wall, and then there's this: an okay WAL.

**WARNING: This crate is early in development. Please do not use in any
production projects until this has been incorporated into
[Sediment](https://github.com/khonsulabs/sediment) and shipping as part of
[Nebari](https://github.com/khonsulabs/nebari). The file format is currently
considered unstable.**

![okaywal forbids unsafe code](https://img.shields.io/badge/unsafe-forbid-success)
[![crate version](https://img.shields.io/crates/v/okaywal.svg)](https://crates.io/crates/okaywal)
[![Live Build Status](https://img.shields.io/github/actions/workflow/status/khonsulabs/okaywal/rust.yml?branch=main)](https://github.com/khonsulabs/okaywal/actions?query=workflow:Tests)
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
in a given directory. To open a log, an implementer of
[`LogManager`](https://khonsulabs.github.io/okaywal/main/okaywal/trait.LogManager.html) must be provided. This trait is how
OkayWAL communicates with your code when recovering or checkpointing a log.

The [basic example][basic-example] shows this process with many comments
describing how OkayWAL works.

```rust,ignore
// Open a log using a Checkpointer that echoes the information passed into each
// function that the Checkpointer trait defines.
let log = WriteAheadLog::recover("my-log", LoggingCheckpointer)?;

// Begin writing an entry to the log.
let mut writer = log.begin_entry()?;

// Each entry is one or more chunks of data. Each chunk can be individually
// addressed using its LogPosition.
let record = writer.write_chunk("this is the first entry".as_bytes())?;

// To fully flush all written bytes to disk and make the new entry
// resilient to a crash, the writer must be committed.
writer.commit()?;
```

## Multi-Threaded Writing

Optimized writing to the log from multiple threads is handled automatically.
Only one thread may access the active log file at any moment in time. Because
the slowest part of writing data to disk is `fsync`, OkayWAL manages
synchronizing multiple writers such that a single `fsync` call can be made for
multiple writes.

This can be demonstrated by running the benchmark suite: `cargo bench -p
benchmarks`:

### commit-256B

| Label       | avg     | min     | max     | stddev  | out%   |
|-------------|---------|---------|---------|---------|--------|
| okaywal-01t | 1.001ms | 617.5us | 7.924ms | 557.3us | 0.016% |
| okaywal-02t | 1.705ms | 617.3us | 11.38ms | 912.1us | 0.006% |
| okaywal-04t | 1.681ms | 622.4us | 9.688ms | 671.4us | 0.021% |
| okaywal-08t | 1.805ms | 656.5us | 13.88ms | 1.001ms | 0.014% |
| okaywal-16t | 1.741ms | 643.2us | 7.895ms | 796.4us | 0.028% |

### commit-1KB

| Label       | avg     | min     | max     | stddev  | out%   |
|-------------|---------|---------|---------|---------|--------|
| okaywal-01t | 959.3us | 621.9us | 7.419ms | 584.4us | 0.012% |
| okaywal-02t | 1.569ms | 627.5us | 7.986ms | 1.007ms | 0.028% |
| okaywal-04t | 1.856ms | 650.5us | 11.14ms | 1.087ms | 0.017% |
| okaywal-08t | 2.054ms | 697.3us | 11.04ms | 1.066ms | 0.021% |
| okaywal-16t | 1.875ms | 641.5us | 8.193ms | 674.6us | 0.032% |

### commit-4KB

| Label       | avg     | min     | max     | stddev  | out%   |
|-------------|---------|---------|---------|---------|--------|
| okaywal-01t | 1.242ms | 748.8us | 6.902ms | 982.4us | 0.008% |
| okaywal-02t | 1.767ms | 761.9us | 8.986ms | 902.1us | 0.016% |
| okaywal-04t | 2.347ms | 787.1us | 8.853ms | 1.084ms | 0.016% |
| okaywal-08t | 2.798ms | 810.8us | 12.53ms | 1.168ms | 0.014% |
| okaywal-16t | 2.151ms | 840.5us | 14.74ms | 1.201ms | 0.008% |

### commit-1MB

| Label       | avg     | min     | max     | stddev  | out%   |
|-------------|---------|---------|---------|---------|--------|
| okaywal-01t | 7.018ms | 5.601ms | 9.865ms | 788.2us | 0.027% |
| okaywal-02t | 11.06ms | 4.281ms | 20.14ms | 3.521ms | 0.000% |
| okaywal-04t | 19.77ms | 5.094ms | 73.21ms | 8.794ms | 0.007% |
| okaywal-08t | 25.06ms | 2.871ms | 97.60ms | 17.33ms | 0.002% |
| okaywal-16t | 19.01ms | 3.480ms | 58.85ms | 7.195ms | 0.014% |

These numbers are the time taken for a single thread to perform an atomic and
durable write of a given size to the log file. Despite using a single-file
approach, we are able to keep average write times very low even with a large
number of simultaneous writers.

## How OkayWAL works

OkayWAL streams incoming data into "segments". Each segment file is named with
the format `wal-{id}`. The id of a segment file refers to the first `EntryId`
that could appear within the segment file.

Segment files are pre-allocated to the length configured in
`Configuration::preallocate_bytes`. Preallocating files is critical for
performance, as overwriting existing bytes in general is less expensive than
allocating new bytes on disk.

OkayWAL always has a current segment file. When a new entry is written, it
always goes to the current segment file. When an entry is completed, the length
of the segment file is checked against `Configuration::checkpoint_after_bytes`.
If enough data has been written to trigger a checkpoint, the file is sent to the
checkpointing thread and a new segment file is activated.

Regardless of whether the file is checkpointed, before control returns from
committing an entry, the file is `fsync`ed. `fsync` operations are batched,
allowing multiple entries to be written by separate threads during the same
`fsync` operation.

OkayWAL also keeps track of any time a new file is created or a file is renamed.
As needed, the directory containing the write-ahead logs is also `fsync`ed to
ensure necessary file and directory metadata is fully synchronized. Just like
file `fsync` batching, OkayWAL also automatically batches directory `fsync`s
across threads.

### Checkpointing a segment file (Background Thread)

The checkpointing thread holds a weak reference to the `WriteAheadLog` data.
When a file is received by the thread to checkpoint, it will upgrade the weak
reference. If it cannot, the checkpointing thread shuts down gracefully and the
recovery process will send the file again for checkpointing the next time the
log is opened.

The thread invokes `LogManager::checkpoint_to` for the file, allowing the
`LogManager` to make any needed changes to persist the data stored in the
segment being checkpointed.

After the `LogManager` finishes, the file is renamed to include `-cp` as its
suffix. Until this step, readers are able to be opened against data stored in
the file being checkpointed. Once the file is renamed, new readers will begin
returning not found errors.

After the file is renamed, the checkpointer waits for all outstanding readers to
finish reading data. The file is then finally recycled by moving it to the
inactive files list.

### Activating a new segment file

If there are any files in the inactive files list, one is reused. Otherwise, a
new file is created and filled with 0's to the configured preallocation length.

The file's name is set to `wal-{next EntryId}`. For example, a brand new
write-ahead log's first segment file will be named `wal-1`, and the first
`EntryId` written will be `1`.

### Segment File Format

Each segment file starts with this header:

- `okw`: Three byte magic code
- OkayWAL Version: Single byte version number. Currently 0.
- `Configuration::version_info` length: Single byte. The embedded information
  must be 255 or less bytes long.
- Embedded Version Info: The bytes of the version info. The previous byte
  controls how many bytes long this field is.

After this header, the file is a series of entries, each which contain a series
of chunks. A byte with a value of 1 signifies a new entry. Any other byte causes
the reader to stop reading entries from the file.

The first 8 bytes of the entry are the little-endian representation of its
`EntryId`.

After the `EntryId`, a series of chunks is expected. A byte with a value of 2
signals that a chunk is next in the file. A byte with a value of 3 signals that
this is the end of the current entry being written. Any other byte causes the
`SegmentReader` to return an AbortedEntry result. Any already-read chunks from
this entry should be ignored/rolled back by the `LogManager`.

The first four bytes of a chunk are the data length in little-endian
representation. The data for the chunk follows.

Finally, a four-byte CRC-32 ends the chunk.

If a reader does not encounter a new chunk marker (2) or an end-of-entry marker
(3), the entry should be considered abandoned and all chunks should be ignored.

[basic-example]: https://github.com/khonsulabs/okaywal/blob/main/examples/basic.rs
[wal]: https://en.wikipedia.org/wiki/Write-ahead_logging

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
