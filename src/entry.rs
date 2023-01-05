use std::io::{self, Read, Write};

use crc32c::crc32c_append;
use parking_lot::MutexGuard;

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
        let mut writer = self.begin_chunk(u32::try_from(data.len()).to_io()?)?;
        writer.write_all(data)?;
        writer.finish()
    }

    /// Begins writing a chunk with the given `length`.
    ///
    /// The writer returned already contains an internal buffer. This function
    /// can be used to write a complex payload without needing to first
    /// combine it in another buffer.
    pub fn begin_chunk(&mut self, length: u32) -> io::Result<ChunkWriter<'_>> {
        let mut file = self.file.as_ref().expect("already dropped").lock();

        let position = LogPosition {
            file_id: file.id(),
            offset: file.position(),
        };

        file.write_all(&[CHUNK])?;
        file.write_all(&length.to_le_bytes())?;

        Ok(ChunkWriter {
            file,
            position,
            length,
            bytes_remaining: length,
            crc32: 0,
            finished: false,
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

pub struct ChunkWriter<'a> {
    file: MutexGuard<'a, LogFileWriter>,
    position: LogPosition,
    length: u32,
    bytes_remaining: u32,
    crc32: u32,
    finished: bool,
}

impl<'a> ChunkWriter<'a> {
    pub fn finish(mut self) -> io::Result<ChunkRecord> {
        self.write_tail()?;
        Ok(ChunkRecord {
            position: self.position,
            crc: self.crc32,
            length: self.length,
        })
    }

    fn write_tail(&mut self) -> io::Result<()> {
        self.finished = true;

        if self.bytes_remaining != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "written length does not match expected length",
            ));
        }

        self.file.write_all(&self.crc32.to_le_bytes())
    }
}

impl<'a> Drop for ChunkWriter<'a> {
    fn drop(&mut self) {
        if !self.finished {
            self.write_tail()
                .expect("chunk writer dropped without finishing");
        }
    }
}

impl<'a> Write for ChunkWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_to_write = buf
            .len()
            .min(usize::try_from(self.bytes_remaining).to_io()?);

        let bytes_written = self.file.write(&buf[..bytes_to_write])?;
        if bytes_written > 0 {
            self.bytes_remaining -= u32::try_from(bytes_written).to_io()?;
            self.crc32 = crc32c_append(self.crc32, &buf[..bytes_written]);
        }
        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The position of a chunk of data within a [`WriteAheadLog`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct LogPosition {
    pub(crate) file_id: u64,
    pub(crate) offset: u64,
}

impl LogPosition {
    /// The number of bytes required to serialize a `LogPosition` using
    /// [`LogPosition::serialize_to()`].
    pub const SERIALIZED_LENGTH: u8 = 16;

    /// Serializes this position to `destination`.
    ///
    /// This writes [`LogPosition::SERIALIZED_LENGTH`] bytes to `destination`.
    pub fn serialize_to<W: Write>(&self, mut destination: W) -> io::Result<()> {
        let mut all_bytes = [0; 16];
        all_bytes[..8].copy_from_slice(&self.file_id.to_le_bytes());
        all_bytes[8..].copy_from_slice(&self.offset.to_le_bytes());
        destination.write_all(&all_bytes)
    }

    /// Deserializes a `LogPosition` from `read`.
    ///
    /// This reads [`LogPosition::SERIALIZED_LENGTH`] bytes from `read` and
    /// returns the deserialized log position.
    pub fn deserialize_from<R: Read>(mut read: R) -> io::Result<Self> {
        let mut all_bytes = [0; 16];
        read.read_exact(&mut all_bytes)?;

        let file_id = u64::from_le_bytes(all_bytes[..8].try_into().expect("u64 is 8 bytes"));
        let offset = u64::from_le_bytes(all_bytes[8..].try_into().expect("u64 is 8 bytes"));

        Ok(Self { file_id, offset })
    }
}

#[test]
fn log_position_serialization() {
    let position = LogPosition {
        file_id: 1,
        offset: 2,
    };
    let mut serialized = Vec::new();
    position.serialize_to(&mut serialized).unwrap();
    assert_eq!(
        serialized.len(),
        usize::from(LogPosition::SERIALIZED_LENGTH)
    );
    let deserialized = LogPosition::deserialize_from(&serialized[..]).unwrap();
    assert_eq!(position, deserialized);
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
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default, Hash)]
pub struct EntryId(pub u64);
