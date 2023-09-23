use std::{
    collections::{BTreeMap, HashMap},
    io::{Read, Write},
    iter::repeat_with,
    path::Path,
    sync::Arc,
    time::Duration,
};

use file_manager::{fs::StdFileManager, memory::MemoryFileManager, FileManager};
use parking_lot::Mutex;
use tempfile::tempdir;

use crate::{
    entry::NEW_ENTRY, Configuration, Entry, EntryId, LogManager, RecoveredSegment, Recovery,
    SegmentReader, WriteAheadLog,
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
        data: HashMap<EntryId, Vec<Vec<u8>>>,
    },
}

impl<M> LogManager<M> for LoggingCheckpointer
where
    M: file_manager::FileManager,
{
    fn should_recover_segment(&mut self, segment: &RecoveredSegment) -> std::io::Result<Recovery> {
        let mut invocations = self.invocations.lock();
        invocations.push(CheckpointCall::ShouldRecoverSegment {
            version_info: segment.version_info.clone(),
        });

        Ok(Recovery::Recover)
    }

    fn recover(&mut self, entry: &mut Entry<'_, M::File>) -> std::io::Result<()> {
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
        _last_checkpointed_id: EntryId,
        entries: &mut SegmentReader<M::File>,
        _wal: &WriteAheadLog<M>,
    ) -> std::io::Result<()> {
        let mut invocations = self.invocations.lock();
        let mut data = HashMap::new();
        while let Some(mut entry) = entries.read_entry()? {
            if let Some(entry_chunks) = entry.read_all_chunks()? {
                data.insert(entry.id(), entry_chunks);
            }
        }
        invocations.push(CheckpointCall::Checkpoint { data });

        Ok(())
    }
}

fn basic<M: FileManager, P: AsRef<Path>>(manager: M, path: P) {
    let checkpointer = LoggingCheckpointer::default();

    let message = b"hello world";

    let config = Configuration::default_with_manager(path, manager);

    let wal = config.clone().open(checkpointer.clone()).unwrap();
    let mut writer = wal.begin_entry().unwrap();
    let record = writer.write_chunk(message).unwrap();
    let written_entry_id = writer.commit().unwrap();
    println!("hello world written to {record:?} in {written_entry_id:?}");
    drop(wal);

    let invocations = checkpointer.invocations.lock();
    assert!(invocations.is_empty());
    drop(invocations);

    let wal = config.open(checkpointer.clone()).unwrap();

    let invocations = checkpointer.invocations.lock();
    println!("Invocations: {invocations:?}");
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
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).unwrap();
    assert_eq!(buffer, message);
    assert!(reader.crc_is_valid().expect("error validating crc"));
    drop(reader);

    let mut writer = wal.begin_entry().unwrap();
    let _ = writer.write_chunk(message).unwrap();
    let written_entry_id = writer.commit().unwrap();

    wal.checkpoint_active()
        .expect("Could not checkpoint active log");
    wal.shutdown().unwrap();

    let invocations = checkpointer.invocations.lock();
    println!("Invocations: {invocations:?}");
    assert_eq!(invocations.len(), 3);
    match &invocations[2] {
        CheckpointCall::Checkpoint { data } => {
            let item = data.get(&written_entry_id).expect(&format!(
                "Could not find checkpointed entry: {:?}",
                written_entry_id
            ));
            assert_eq!(item.len(), 1);
            assert_eq!(item[0], message);
        }
        other => unreachable!("unexpected invocation: {other:?}"),
    }
    drop(invocations);
}

#[test]
fn basic_std() {
    let dir = tempdir().unwrap();
    basic(StdFileManager::default(), &dir);
}

#[test]
fn basic_memory() {
    basic(MemoryFileManager::default(), "/");
}

#[derive(Debug, Default, Clone)]
struct VerifyingCheckpointer {
    entries: Arc<Mutex<BTreeMap<EntryId, Vec<Vec<u8>>>>>,
}

impl<M> LogManager<M> for VerifyingCheckpointer
where
    M: file_manager::FileManager,
{
    fn recover(&mut self, entry: &mut Entry<'_, M::File>) -> std::io::Result<()> {
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
        reader: &mut SegmentReader<M::File>,
        _wal: &WriteAheadLog<M>,
    ) -> std::io::Result<()> {
        println!("Checkpointed to {last_checkpointed_id:?}");
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

fn multithreaded<M: FileManager, P: AsRef<Path>>(manager: M, path: P) {
    let mut threads = Vec::new();

    let checkpointer = VerifyingCheckpointer::default();
    let original_entries = checkpointer.entries.clone();
    let wal = Configuration::default_with_manager(path.as_ref(), manager.clone())
        .open(checkpointer)
        .unwrap();

    for _ in 0..5 {
        let wal = wal.clone();
        let written_entries = original_entries.clone();
        threads.push(std::thread::spawn(move || {
            let rng = fastrand::Rng::new();
            for _ in 1..10 {
                let mut messages = Vec::with_capacity(rng.usize(1..=8));
                let mut writer = wal.begin_entry().unwrap();
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
    let _wal = Configuration::default_with_manager(path.as_ref(), manager)
        .open(checkpointer)
        .unwrap();
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
fn multithreaded_std() {
    let dir = tempdir().unwrap();
    multithreaded(StdFileManager::default(), &dir);
}

#[test]
fn multithreaded_memory() {
    multithreaded(MemoryFileManager::default(), "/");
}

fn aborted_entry<M: FileManager, P: AsRef<Path>>(manager: M, path: P) {
    let checkpointer = LoggingCheckpointer::default();

    let message = b"hello world";

    let wal = Configuration::default_with_manager(path.as_ref(), manager.clone())
        .open(checkpointer.clone())
        .unwrap();
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

    Configuration::default_with_manager(path.as_ref(), manager)
        .open(checkpointer.clone())
        .unwrap();

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
fn aborted_entry_std() {
    let dir = tempdir().unwrap();
    aborted_entry(StdFileManager::default(), &dir);
}

#[test]
fn aborted_entry_memory() {
    aborted_entry(MemoryFileManager::default(), "/");
}

fn always_checkpointing<M: FileManager, P: AsRef<Path>>(manager: M, path: P) {
    let checkpointer = LoggingCheckpointer::default();
    let config =
        Configuration::default_with_manager(path.as_ref(), manager).checkpoint_after_bytes(33);

    let mut written_chunks = Vec::new();
    for i in 0_usize..100 {
        println!("About to insert {i}");
        let wal = config.clone().open(checkpointer.clone()).unwrap();
        let mut writer = wal.begin_entry().unwrap();
        println!("Writing {i}");
        let record = writer.write_chunk(&i.to_be_bytes()).unwrap();
        let written_entry_id = writer.commit().unwrap();
        println!("{i} written to {record:?} in {written_entry_id:?}");
        written_chunks.push((written_entry_id, record));
        wal.shutdown().unwrap();
    }

    // Because we write and close the database so quickly, it's possible for the
    // final write to not have been checkpointed by the background thread. So,
    // we'll reopen the WAL one more time and sleep for a moment to allow it to
    // finish.
    let _wal = config.open(checkpointer.clone()).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    let invocations = checkpointer.invocations.lock();
    println!("Invocations: {invocations:?}");

    for (index, (entry_id, _)) in written_chunks.into_iter().enumerate() {
        println!("Checking {index}");
        let chunk_data = invocations
            .iter()
            .find_map(|call| {
                if let CheckpointCall::Checkpoint { data, .. } = call {
                    data.get(&entry_id)
                } else {
                    None
                }
            })
            .expect("entry not checkpointed");

        assert_eq!(chunk_data[0], index.to_be_bytes());
    }
}

#[test]
fn always_checkpointing_std() {
    let dir = tempdir().unwrap();
    always_checkpointing(StdFileManager::default(), &dir);
}

#[test]
fn always_checkpointing_memory() {
    always_checkpointing(MemoryFileManager::default(), "/");
}
