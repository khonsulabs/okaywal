use std::io::{self, Read};

use okaywal::{Archiver, Entry, EntryId, RecoveredSegment, Recovery, WriteAheadLog};

fn main() -> io::Result<()> {
    // Open a log using an Archiver that echoes the information passed into each
    // function that the Archiver trait defines.
    let log = WriteAheadLog::recover("my-log", LoggingArchiver)?;

    // Begin writing an entry to the log.
    let mut writer = log.write()?;

    // Each entry is one or more chunks of data. Each chunk can be individually
    // addressed using its LogPosition.
    let record = writer.write_all("this is the first entry".as_bytes())?;

    // To fully flush all written bytes to disk and make the new entry
    // resilliant to a crash, the writer must be committed.
    writer.commit()?;

    // Let's reopen the log. During this process,
    // LoggingArchiver::should_recover_segment will be invoked for each segment
    // file that has not been archived yet. In this example, it will be called
    // once. Once the Archiver confirms the data should be recovered,
    // LoggingArchiver::recover will be invoked once for each entry in the WAL
    // that hasn't been previously archived.
    drop(log);
    let log = WriteAheadLog::recover("my-log", LoggingArchiver)?;

    // We can use the previously returned DataRecord to read the original data.
    let mut reader = log.read_at(record.position)?;
    let mut buffer = vec![0; usize::try_from(record.length).unwrap()];
    reader.read_exact(&mut buffer)?;
    println!(
        "Data read from log: {}",
        String::from_utf8(buffer).expect("invalid utf-8")
    );

    // Cleanup
    drop(reader);
    drop(log);
    std::fs::remove_dir_all("my-log")?;

    Ok(())
}

#[derive(Debug)]
struct LoggingArchiver;

impl Archiver for LoggingArchiver {
    fn should_recover_segment(&mut self, segment: &RecoveredSegment) -> io::Result<Recovery> {
        println!(
            "LogingArchiver::should_recover_segment({:?})",
            segment.version_info
        );

        Ok(Recovery::Recover)
    }

    fn recover(&mut self, entry: &mut Entry<'_>) -> io::Result<()> {
        let mut all_data = Vec::new();
        while let Some(mut chunk) = entry.read_chunk()? {
            let data = chunk.read_all()?;
            all_data.push(String::from_utf8(data).expect("invalid utf-8"));
        }

        println!(
            "LogingArchiver::recover(entry_id: {:?}, data: {:?})",
            entry.id, all_data,
        );

        Ok(())
    }

    fn archive(&mut self, last_archived_id: EntryId) -> io::Result<()> {
        println!("LogingArchiver::recover({last_archived_id:?}");
        Ok(())
    }
}

#[test]
fn test() -> io::Result<()> {
    // Clean up any previous runs of this example.
    let path = std::path::Path::new("my-log");
    if path.exists() {
        std::fs::remove_dir_all("my-log")?;
    }

    main()
}
