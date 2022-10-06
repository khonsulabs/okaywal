use std::{
    collections::BTreeMap,
    io::{Read, Write},
    iter::repeat_with,
    sync::Arc,
};

use parking_lot::Mutex;
use tempfile::tempdir;

use crate::{
    entry::NEW_ENTRY, Entry, EntryId, LogManager, RecoveredSegment, Recovery, SegmentReader,
    WriteAheadLog,
};

#[derive(Default, Debug, Clone)]
struct LoggingCheckpointer {
    invocations: Arc<Mutex<Vec<CheckpointCall>>>,
}

#[derive(Debug)]
enum CheckpointCall {
    ShouldRecoverSegment {
        version_info: Vec<u8>,
    },
    Recover {
        entry_id: EntryId,
        data: Vec<Vec<u8>>,
    },
    Checkpoint {
        last_checkpointed_id: EntryId,
    },
}

impl LogManager for LoggingCheckpointer {
    fn should_recover_segment(&mut self, segment: &RecoveredSegment) -> std::io::Result<Recovery> {
        let mut invocations = self.invocations.lock();
        invocations.push(CheckpointCall::ShouldRecoverSegment {
            version_info: segment.version_info.clone(),
        });

        Ok(Recovery::Recover)
    }

    fn recover(&mut self, entry: &mut Entry<'_>) -> std::io::Result<()> {
        let entry_id = dbg!(entry.id);

        if let Some(chunks) = entry.read_all_chunks()? {
            let mut invocations = self.invocations.lock();
            invocations.push(CheckpointCall::Recover {
                entry_id,
                data: chunks,
            });
        }

        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: EntryId,
        _entries: &mut SegmentReader,
    ) -> std::io::Result<()> {
        let mut invocations = self.invocations.lock();
        invocations.push(CheckpointCall::Checkpoint {
            last_checkpointed_id,
        });

        Ok(())
    }
}

#[test]
fn basic() {
    let dir = tempdir().unwrap();

    let checkpointer = LoggingCheckpointer::default();

    let message = b"hello world";

    let wal = WriteAheadLog::recover(&dir, checkpointer.clone()).unwrap();
    let mut writer = wal.begin_entry().unwrap();
    let record = writer.write_chunk(message).unwrap();
    let written_entry_id = writer.commit().unwrap();
    println!("hello world written to {record:?} in {written_entry_id:?}");
    drop(wal);

    let invocations = checkpointer.invocations.lock();
    assert!(invocations.is_empty());
    drop(invocations);

    let wal = WriteAheadLog::recover(&dir, checkpointer.clone()).unwrap();

    let invocations = checkpointer.invocations.lock();
    assert_eq!(invocations.len(), 2);
    match &invocations[0] {
        CheckpointCall::ShouldRecoverSegment { version_info } => {
            assert!(version_info.is_empty());
        }
        other => unreachable!("unexpected invocation: {other:?}"),
    }
    match &invocations[1] {
        CheckpointCall::Recover { entry_id, data } => {
            assert_eq!(written_entry_id, *entry_id);
            assert_eq!(data.len(), 1);
            assert_eq!(data[0], message);
        }
        other => unreachable!("unexpected invocation: {other:?}"),
    }
    drop(invocations);

    let mut reader = wal.read_at(record.position).unwrap();
    let mut buffer = vec![0; message.len()];
    reader.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, message);
}

#[derive(Debug, Default, Clone)]
struct VerifyingCheckpointer {
    entries: Arc<Mutex<BTreeMap<EntryId, Vec<Vec<u8>>>>>,
}

impl LogManager for VerifyingCheckpointer {
    fn recover(&mut self, entry: &mut Entry<'_>) -> std::io::Result<()> {
        dbg!(entry.id);
        if let Some(chunks) = entry.read_all_chunks()? {
            let mut entries = self.entries.lock();
            entries.insert(dbg!(entry.id), chunks);
        }

        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: EntryId,
        reader: &mut SegmentReader,
    ) -> std::io::Result<()> {
        println!(
            "Checkpointed {} to {last_checkpointed_id:?}",
            reader.path.display()
        );
        let mut entries = self.entries.lock();
        while let Some(mut entry) = reader.read_entry().unwrap() {
            let expected_data = entries.remove(&entry.id).expect("unknown entry id");
            let stored_data = entry
                .read_all_chunks()
                .unwrap()
                .expect("encountered aborted entry");
            assert_eq!(expected_data, stored_data);
        }
        entries.retain(|entry_id, _| *entry_id > last_checkpointed_id);
        Ok(())
    }
}

#[test]
fn multithreaded() {
    let dir = tempdir().unwrap();

    let mut threads = Vec::new();

    let checkpointer = VerifyingCheckpointer::default();
    let original_entries = checkpointer.entries.clone();
    let wal = WriteAheadLog::recover(&dir, checkpointer).unwrap();

    for _ in 0..5 {
        let wal = wal.clone();
        let written_entries = original_entries.clone();
        threads.push(std::thread::spawn(move || {
            let rng = fastrand::Rng::new();
            for _ in 1..100 {
                let mut messages = Vec::with_capacity(rng.usize(1..=8));
                let mut writer = wal.begin_entry().unwrap();
                println!("Writing to {}", writer.path().display());
                for _ in 0..messages.capacity() {
                    let message = repeat_with(|| 42)
                        .take(rng.usize(..65_536))
                        .collect::<Vec<_>>();
                    // let message = vec![42; 256];
                    writer.write_chunk(&message).unwrap();
                    messages.push(message);
                }
                // Lock entries before pushing the commit to ensure that a
                // checkpoint operation can't happen before we insert this
                // entry.
                let mut entries = written_entries.lock();
                entries.insert(writer.id(), messages);
                drop(entries);
                writer.commit().unwrap();
            }
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    wal.shutdown().unwrap();

    println!("Reopening log");
    let checkpointer = VerifyingCheckpointer::default();
    let recovered_entries = checkpointer.entries.clone();
    let _wal = WriteAheadLog::recover(&dir, checkpointer).unwrap();
    let recovered_entries = recovered_entries.lock();
    let original_entries = original_entries.lock();
    // Check keys first because it's easier to verify a list of ids than it is
    // to look at debug output of a bunch of bytes.
    assert_eq!(
        original_entries.keys().collect::<Vec<_>>(),
        recovered_entries.keys().collect::<Vec<_>>()
    );
    assert_eq!(&*original_entries, &*recovered_entries);
}

#[test]
fn aborted_entry() {
    let dir = tempdir().unwrap();

    let checkpointer = LoggingCheckpointer::default();

    let message = b"hello world";

    let wal = WriteAheadLog::recover(&dir, checkpointer.clone()).unwrap();
    let mut writer = wal.begin_entry().unwrap();
    let record = writer.write_chunk(message).unwrap();
    let written_entry_id = writer
        .commit_and(|file| file.write_all(&[NEW_ENTRY]))
        .unwrap();
    println!("hello world written to {record:?} in {written_entry_id:?}");
    drop(wal);

    let invocations = checkpointer.invocations.lock();
    assert!(invocations.is_empty());
    drop(invocations);

    WriteAheadLog::recover(&dir, checkpointer.clone()).unwrap();

    let invocations = checkpointer.invocations.lock();
    assert_eq!(invocations.len(), 2);
    match &invocations[0] {
        CheckpointCall::ShouldRecoverSegment { version_info } => {
            assert!(version_info.is_empty());
        }
        other => unreachable!("unexpected invocation: {other:?}"),
    }
    match &invocations[1] {
        CheckpointCall::Recover { entry_id, data } => {
            assert_eq!(written_entry_id, *entry_id);
            assert_eq!(data.len(), 1);
            assert_eq!(data[0], message);
        }
        other => unreachable!("unexpected invocation: {other:?}"),
    }
    drop(invocations);
}
