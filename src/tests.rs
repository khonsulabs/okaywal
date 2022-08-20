use std::{
    collections::BTreeMap,
    io::{Read, Write},
    iter::repeat_with,
    sync::Arc,
};

use parking_lot::Mutex;
use tempfile::tempdir;

use crate::{
    file::{File, LockedFile},
    Checkpointer, Entry, EntryId, Error, LogReader, Recovery, WriteAheadLog, NEW_ENTRY,
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

impl Checkpointer for LoggingCheckpointer {
    fn should_recover_segment(
        &mut self,
        segment: &crate::RecoveredSegment,
    ) -> std::io::Result<Recovery> {
        let mut invocations = self.invocations.lock();
        invocations.push(CheckpointCall::ShouldRecoverSegment {
            version_info: segment.version_info.clone(),
        });

        Ok(Recovery::Recover)
    }

    fn recover(&mut self, entry: &mut Entry<'_, File>) -> std::io::Result<()> {
        let entry_id = entry.id;

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
        _entries: &mut LogReader<LockedFile>,
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
    let mut writer = wal.write().unwrap();
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

#[derive(Debug, Default)]
struct VerifyingCheckpointer {
    entries: Arc<Mutex<BTreeMap<EntryId, Vec<Vec<u8>>>>>,
}

impl Checkpointer for VerifyingCheckpointer {
    fn should_recover_segment(
        &mut self,
        _segment: &crate::RecoveredSegment,
    ) -> std::io::Result<Recovery> {
        Ok(Recovery::Recover)
    }

    fn recover(&mut self, entry: &mut Entry<'_, File>) -> std::io::Result<()> {
        if let Some(chunks) = entry.read_all_chunks()? {
            let mut entries = self.entries.lock();
            entries.insert(entry.id, chunks);
        }

        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: EntryId,
        reader: &mut LogReader<LockedFile>,
    ) -> std::io::Result<()> {
        println!("Checkpointed to {last_checkpointed_id:?}");
        let mut entries = self.entries.lock();
        while let Some(mut entry) = reader.read_entry()? {
            let expected_data = entries.remove(&entry.id).expect("unknown entry id");
            let stored_data = entry.read_all_chunks()?.expect("couldn't read chunks");
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

    for _ in 0..8 {
        let wal = wal.clone();
        let written_entries = original_entries.clone();
        threads.push(std::thread::spawn(move || {
            let rng = fastrand::Rng::new();
            for _ in 1..100 {
                let mut messages = Vec::with_capacity(rng.usize(1..=8));
                let mut writer = wal.write().unwrap();
                for _ in 0..messages.capacity() {
                    let message = repeat_with(|| rng.u8(..))
                        .take(rng.usize(..65_536))
                        .collect::<Vec<_>>();
                    writer.write_chunk(&message).unwrap();
                    messages.push(message);
                }
                let entry_id = writer.commit().unwrap();
                let mut entries = written_entries.lock();
                entries.insert(entry_id, messages);
            }
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    drop(wal);

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
    let mut writer = wal.write().unwrap();
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

#[test]
fn shutdown() {
    let dir = tempdir().unwrap();

    let mut threads = Vec::new();

    let checkpointer = VerifyingCheckpointer::default();
    let original_entries = checkpointer.entries.clone();
    let wal = WriteAheadLog::recover(&dir, checkpointer).unwrap();

    for _ in 0..8 {
        let wal = wal.clone();
        let written_entries = original_entries.clone();
        threads.push(std::thread::spawn(move || -> crate::Result<()> {
            let rng = fastrand::Rng::new();
            for _ in 1..100 {
                let mut messages = Vec::with_capacity(rng.usize(1..=8));
                let mut writer = wal.write()?;
                for _ in 0..messages.capacity() {
                    let message = repeat_with(|| rng.u8(..))
                        .take(rng.usize(..65_536))
                        .collect::<Vec<_>>();
                    writer.write_chunk(&message)?;
                    messages.push(message);
                }
                let entry_id = writer.commit()?;
                let mut entries = written_entries.lock();
                entries.insert(entry_id, messages);
            }

            Ok(())
        }));
    }

    wal.shutdown().unwrap();

    for thread in threads {
        assert!(matches!(thread.join().unwrap(), Err(Error::ShuttingDown)));
    }

    drop(wal);

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
