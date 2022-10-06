use std::io::{self, Write};

use crate::{
    log_file::{LogFile, LogFileWriter},
    to_io_result::ToIoResult,
    WriteAheadLog, WriteResult,
};

/// A writer for an entry in a [`WriteAheadLog`].
///
/// Only one writer can be active for a given [`WriteAheadLog`] at any given
/// time. See [`WriteAheadLog::begin_entry()`] for more information.
#[derive(Debug)]
pub struct EntryWriter<'a> {
    id: EntryId,
    log: &'a WriteAheadLog,
    file: Option<LogFile>,
    original_length: u64,
}

pub const NEW_ENTRY: u8 = 1;
pub const CHUNK: u8 = 2;
pub const END_OF_ENTRY: u8 = 3;

impl<'a> EntryWriter<'a> {
    pub(super) fn new(log: &'a WriteAheadLog, id: EntryId, file: LogFile) -> io::Result<Self> {
        let mut writer = file.lock();
        let original_length = writer.position();

        writer.write_all(&[NEW_ENTRY])?;
        writer.write_all(&id.0.to_le_bytes())?;
        drop(writer);

        Ok(Self {
            id,
            log,
            file: Some(file),
            original_length,
        })
    }

    /// Returns the unique id of the log entry being written.
    #[must_use]
    pub const fn id(&self) -> EntryId {
        self.id
    }

    /// Commits this entry to the log. Once this call returns, all data is
    /// atomically updated and synchronized to disk.
    ///
    /// While the entry is being committed, other writers will be allowed to
    /// write to the log. See [`WriteAheadLog::begin_entry()`] for more
    /// information.
    pub fn commit(self) -> io::Result<EntryId> {
        self.commit_and(|_file| Ok(()))
    }

    pub(crate) fn commit_and<F: FnOnce(&mut LogFileWriter) -> io::Result<()>>(
        mut self,
        callback: F,
    ) -> io::Result<EntryId> {
        let file = self.file.take().expect("already committed");

        let mut writer = file.lock();

        writer.write_all(&[END_OF_ENTRY])?;
        let new_length = writer.position();
        callback(&mut writer)?;
        writer.set_last_entry_id(Some(self.id));
        drop(writer);

        self.log.reclaim(file, WriteResult::Entry { new_length })?;

        Ok(self.id)
    }

    /// Abandons this entry, preventing the entry from being recovered in the
    /// future. This is automatically done when dropped, but errors that occur
    /// during drop will panic.
    pub fn rollback(mut self) -> io::Result<()> {
        self.rollback_session()
    }

    fn rollback_session(&mut self) -> io::Result<()> {
        let file = self.file.take().expect("file already dropped");

        let mut writer = file.lock();
        writer.revert_to(self.original_length)?;
        drop(writer);

        self.log.reclaim(file, WriteResult::RolledBack).unwrap();

        Ok(())
    }

    /// Appends a chunk of data to this log entry. Each chunk of data is able to
    /// be read using [`Entry::read_chunk`](crate::Entry).
    pub fn write_chunk(&mut self, data: &[u8]) -> io::Result<ChunkRecord> {
        let length = u32::try_from(data.len()).to_io()?;
        let mut file = self.file.as_ref().expect("already dropped").lock();
        let position = LogPosition {
            file_id: file.id(),
            offset: file.position(),
        };
        let crc = crc32c::crc32c(data);
        let mut header = [CHUNK; 9];
        header[1..5].copy_from_slice(&crc.to_le_bytes());
        header[5..].copy_from_slice(&length.to_le_bytes());
        file.write_all(&header)?;
        file.write_all(data)?;

        Ok(ChunkRecord {
            position,
            crc,
            length,
        })
    }
}

impl<'a> Drop for EntryWriter<'a> {
    fn drop(&mut self) {
        if self.file.is_some() {
            self.rollback_session().unwrap();
        }
    }
}

/// The position of a chunk of data within a [`WriteAheadLog`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct LogPosition {
    pub(crate) file_id: u64,
    pub(crate) offset: u64,
}

/// A record of a chunk that was written to a [`WriteAheadLog`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ChunkRecord {
    /// The position of the chunk.
    pub position: LogPosition,
    /// The crc calculated for the chunk.
    pub crc: u32,
    /// The length of the data contained inside of the chunk.
    pub length: u32,
}

/// The unique id of an entry written to a [`WriteAheadLog`]. These IDs are
/// ordered by the time the [`EntryWriter`] was created for the entry written with this id.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default)]
pub struct EntryId(pub u64);
