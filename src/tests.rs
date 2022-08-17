use std::{io::Read, sync::Arc};

use parking_lot::Mutex;
use tempfile::{tempdir, tempdir_in};

use crate::{Archiver, Entry, EntryId, Recovery, WriteAheadLog};

#[derive(Default, Debug, Clone)]
struct LoggingArchiver {
    invocations: Arc<Mutex<Vec<ArchiveCall>>>,
}

#[derive(Debug)]
enum ArchiveCall {
    ShouldRecoverSegment {
        version_info: Vec<u8>,
    },
    Recover {
        entry_id: EntryId,
        data: Vec<Vec<u8>>,
    },
    Archive {
        last_archived_id: EntryId,
    },
}

impl Archiver for LoggingArchiver {
    fn should_recover_segment(
        &mut self,
        segment: &crate::RecoveredSegment,
    ) -> std::io::Result<Recovery> {
        let mut invocations = self.invocations.lock();
        invocations.push(ArchiveCall::ShouldRecoverSegment {
            version_info: segment.version_info.clone(),
        });

        Ok(Recovery::Recover)
    }

    fn recover(&mut self, entry: &mut Entry<'_>) -> std::io::Result<()> {
        let entry_id = entry.id;

        let mut invocations = self.invocations.lock();
        let mut chunks = Vec::new();
        while let Some(mut chunk) = entry.read_chunk()? {
            let mut data = Vec::new();
            chunk.read_to_end(&mut data)?;
            assert!(chunk.check_crc().expect("data should be finished"));
            chunks.push(data);
        }
        invocations.push(ArchiveCall::Recover {
            entry_id,
            data: chunks,
        });

        Ok(())
    }

    fn archive(&mut self, last_archived_id: EntryId) -> std::io::Result<()> {
        let mut invocations = self.invocations.lock();
        invocations.push(ArchiveCall::Archive { last_archived_id });

        Ok(())
    }
}

#[test]
fn basic() {
    let dir = tempdir().unwrap();

    let archiver = LoggingArchiver::default();

    let message = b"hello world";

    let wal = WriteAheadLog::recover(&dir, archiver.clone()).unwrap();
    let mut writer = wal.write().unwrap();
    let record = writer.write_all(message).unwrap();
    let written_entry_id = writer.commit().unwrap();
    println!("hello world written to {record:?} in {written_entry_id:?}");
    drop(wal);

    let invocations = archiver.invocations.lock();
    assert!(invocations.is_empty());
    drop(invocations);

    let wal = WriteAheadLog::recover(&dir, archiver.clone()).unwrap();

    let invocations = archiver.invocations.lock();
    assert_eq!(invocations.len(), 2);
    match &invocations[0] {
        ArchiveCall::ShouldRecoverSegment { version_info } => {
            assert!(version_info.is_empty());
        }
        other => unreachable!("unexpected invocation: {other:?}"),
    }
    match &invocations[1] {
        ArchiveCall::Recover { entry_id, data } => {
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

// #[test]
// fn multithreaded() {
//     let dir = tempdir_in(".").unwrap();

//     let archiver = LoggingArchiver::default();
//     let mut threads = Vec::new();
//     let message = b"hello world";

//     let wal = WriteAheadLog::recover(&dir, archiver).unwrap();

//     let (measurements, stats) = timings::Timings::new();

//     for _ in 0..4 {
//         let wal = wal.clone();
//         let measurements = measurements.clone();
//         threads.push(std::thread::spawn(move || {
//             for i in 1..1000 {
//                 let timing = measurements.begin("okaywal", "tx");
//                 let mut writer = wal.write().unwrap();
//                 writer.write_all(message).unwrap();
//                 writer.commit().unwrap();
//                 timing.finish();
//                 println!("Transaction {i} finished");
//             }
//         }));
//     }
//     drop(measurements);
//     drop(wal);

//     for thread in threads {
//         thread.join().unwrap();
//     }

//     let results = stats.join().unwrap();
//     timings::print_table_summaries(&results).unwrap();
// }
