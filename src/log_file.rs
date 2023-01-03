use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::{
    buffered::Buffered,
    entry::{EntryId, CHUNK, END_OF_ENTRY, NEW_ENTRY},
    to_io_result::ToIoResult,
    Configuration, LogPosition,
};

#[derive(Debug, Clone)]
pub struct LogFile {
    data: Arc<LogFileData>,
}

impl LogFile {
    pub fn write(
        id: u64,
        path: PathBuf,
        validated_length: u64,
        last_entry_id: Option<EntryId>,
        config: &Configuration,
    ) -> io::Result<Self> {
        let writer = LogFileWriter::new(id, path, validated_length, last_entry_id, config)?;
        let created_at = if last_entry_id.is_some() {
            None
        } else {
            Some(Instant::now())
        };
        Ok(Self {
            data: Arc::new(LogFileData {
                writer: Mutex::new(writer),
                sync: Condvar::new(),
                created_at,
            }),
        })
    }

    pub fn created_at(&self) -> Option<Instant> {
        self.data.created_at
    }

    pub fn lock(&self) -> MutexGuard<'_, LogFileWriter> {
        self.data.writer.lock()
    }

    pub fn rename(&self, new_name: &str) -> io::Result<()> {
        let mut writer = self.data.writer.lock();
        writer.rename(new_name)
    }
    pub fn synchronize(&self, target_synced_bytes: u64) -> io::Result<()> {
        // Flush the buffer to disk.
        let data = self.lock();
        self.synchronize_locked(data, target_synced_bytes)
            .map(|_| ())
    }

    pub fn synchronize_locked<'a>(
        &'a self,
        mut data: MutexGuard<'a, LogFileWriter>,
        target_synced_bytes: u64,
    ) -> io::Result<MutexGuard<'a, LogFileWriter>> {
        loop {
            if data.synchronized_through >= target_synced_bytes {
                // While we were waiting for the lock, another thread synced our
                // data for us.
                break;
            } else if data.is_syncing {
                // Another thread is currently synchronizing this file.
                self.data.sync.wait(&mut data);
            } else {
                // Become the sync thread for this file.
                data.is_syncing = true;

                // Check if we need to flush the buffer before calling fsync.
                // It's possible that the currently buffered data doesn't need
                // to be flushed.
                if data.buffer_position() < target_synced_bytes {
                    data.file.flush()?;
                }
                let synchronized_length = data.buffer_position();

                // Get a duplicate handle we can use to call sync_data with while the
                // mutex isn't locked.
                let file_to_sync = data.file.inner().try_clone()?;
                drop(data);

                file_to_sync.sync_data()?;

                data = self.lock();
                data.is_syncing = false;
                data.synchronized_through = synchronized_length;
                self.data.sync.notify_all();
                break;
            }
        }

        Ok(data)
    }
}

#[derive(Debug)]
struct LogFileData {
    writer: Mutex<LogFileWriter>,
    created_at: Option<Instant>,
    sync: Condvar,
}

#[derive(Debug)]
pub struct LogFileWriter {
    id: u64,
    path: Arc<PathBuf>,
    file: Buffered<File>,
    last_entry_id: Option<EntryId>,
    version_info: Arc<Vec<u8>>,
    synchronized_through: u64,
    is_syncing: bool,
}

static ZEROES: [u8; 8196] = [0; 8196];

impl LogFileWriter {
    fn new(
        id: u64,
        path: PathBuf,
        validated_length: u64,
        last_entry_id: Option<EntryId>,
        config: &Configuration,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        // Truncate or extend the file to the next multiple of the preallocation
        // length.
        let preallocate_bytes = u64::from(config.preallocate_bytes);
        let padded_length = ((validated_length + preallocate_bytes - 1) / preallocate_bytes).max(1)
            * preallocate_bytes;
        let length = file.seek(SeekFrom::End(0))?;
        let bytes_to_fill = padded_length.checked_sub(length);
        if let Some(bytes_to_fill) = bytes_to_fill {
            let mut bytes_to_fill = usize::try_from(bytes_to_fill).to_io()?;
            // Pre-allocate this disk space by writing zeroes.
            file.set_len(padded_length)?;
            file.seek(SeekFrom::Start(validated_length))?;
            while bytes_to_fill > 0 {
                let bytes_to_write = bytes_to_fill.min(ZEROES.len());
                file.write_all(&ZEROES[..bytes_to_write])?;
                bytes_to_fill -= bytes_to_write;
            }
        }

        // Position the writer to write after the last validated byte.
        file.seek(SeekFrom::Start(validated_length))?;
        let mut file = Buffered::with_capacity(file, config.buffer_bytes)?;

        if validated_length == 0 {
            Self::write_header(&mut file, &config.version_info)?;
            file.flush()?;
        }

        Ok(Self {
            id,
            path: Arc::new(path),
            file,
            last_entry_id,
            version_info: config.version_info.clone(),
            synchronized_through: validated_length,
            is_syncing: false,
        })
    }

    fn write_header(file: &mut Buffered<File>, version_info: &[u8]) -> io::Result<()> {
        file.write_all(b"okw\0")?;
        let version_size = u8::try_from(version_info.len()).to_io()?;
        file.write_all(&[version_size])?;
        file.write_all(version_info)?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn position(&self) -> u64 {
        self.file.position()
    }

    pub fn buffer_position(&self) -> u64 {
        self.file.buffer_position()
    }

    pub fn is_synchronized(&self) -> bool {
        self.file.position() == self.synchronized_through
    }

    pub fn revert_to(&mut self, length: u64) -> io::Result<()> {
        // Reverting doesn't need to change the bits on disk, as long as we
        // gracefully fail when an invalid byte is encountered at the start of
        // an entry.
        self.file.seek(SeekFrom::Start(length))?;
        if self.synchronized_through > length {
            self.synchronized_through = length;
        }
        if self.synchronized_through == 0 {
            Self::write_header(&mut self.file, &self.version_info)?;
            self.last_entry_id = None;
        }

        Ok(())
    }

    pub fn rename(&mut self, new_name: &str) -> io::Result<()> {
        let new_path = self
            .path
            .parent()
            .expect("parent path not found")
            .join(new_name);
        std::fs::rename(&*self.path, &new_path)?;
        self.path = Arc::new(new_path);

        Ok(())
    }

    pub fn last_entry_id(&self) -> Option<EntryId> {
        self.last_entry_id
    }

    pub fn set_last_entry_id(&mut self, last_entry_id: Option<EntryId>) {
        self.last_entry_id = last_entry_id;
    }
}

impl Write for LogFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.file.write(buf)?;

        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Reads a log segment, which contains one or more log entries.
#[derive(Debug)]
pub struct SegmentReader {
    pub(crate) file_id: u64,
    pub(crate) file: BufReader<File>,
    pub(crate) header: RecoveredSegment,
    pub(crate) current_entry_id: Option<EntryId>,
    pub(crate) first_entry_id: Option<EntryId>,
    pub(crate) last_entry_id: Option<EntryId>,
    pub(crate) valid_until: u64,
}

impl SegmentReader {
    pub(crate) fn new(path: &Path, file_id: u64) -> io::Result<Self> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut file = BufReader::new(file);
        let mut buffer = Vec::with_capacity(256 + 5);
        buffer.resize(5, 0);
        file.read_exact(&mut buffer)?;

        if &buffer[0..3] != b"okw" {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "segment file did not contain magic code",
            ));
        }

        if buffer[3] != 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "segment file was written with a newer version",
            ));
        }

        let version_info_length = buffer[4];
        buffer.resize(usize::from(version_info_length), 0);
        file.read_exact(&mut buffer)?;

        let header = RecoveredSegment {
            version_info: buffer,
        };

        Ok(Self {
            file_id,
            file,
            header,
            current_entry_id: None,
            first_entry_id: None,
            last_entry_id: None,
            valid_until: u64::from(version_info_length) + 5,
        })
    }

    fn read_next_entry(&mut self) -> io::Result<bool> {
        self.valid_until = self.file.stream_position()?;
        let mut header_bytes = [0; 9];
        match self.file.read_exact(&mut header_bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(false),
            Err(err) => return Err(err),
        }

        if let NEW_ENTRY = header_bytes[0] {
            let read_id = EntryId(u64::from_le_bytes(header_bytes[1..].try_into().unwrap()));
            if read_id.0 >= self.file_id {
                self.current_entry_id = Some(read_id);
                if self.first_entry_id.is_none() {
                    self.first_entry_id = self.current_entry_id;
                }
                self.last_entry_id = self.current_entry_id;

                return Ok(true);
            }
        }

        self.current_entry_id = None;
        Ok(false)
    }

    /// Reads an entry from the log. If no more entries are found, None is
    /// returned.
    pub fn read_entry(&mut self) -> io::Result<Option<Entry<'_>>> {
        if let Some(id) = self.current_entry_id.take() {
            // Skip the remainder of the current entry.
            let mut entry = Entry { id, reader: self };
            while let Some(chunk) = match entry.read_chunk()? {
                ReadChunkResult::Chunk(chunk) => Some(chunk),
                _ => None,
            } {
                // skip chunk
                chunk.skip_remaining_bytes()?;
            }
        }

        if !self.read_next_entry()? {
            // No more entries.
            return Ok(None);
        }

        Ok(Some(Entry {
            id: self
                .current_entry_id
                .expect("read_next_entry populated this"),
            reader: self,
        }))
    }
}

/// A stored entry inside of a [`WriteAheadLog`](crate::WriteAheadLog).
///
/// Each entry is composed of a series of chunks of data that were previously
/// written using [`EntryWriter::write_chunk`](crate::EntryWriter::write_chunk).
#[derive(Debug)]
pub struct Entry<'a> {
    pub(crate) id: EntryId,
    pub(crate) reader: &'a mut SegmentReader,
}

impl<'entry> Entry<'entry> {
    /// The unique id of this entry.
    #[must_use]
    pub const fn id(&self) -> EntryId {
        self.id
    }

    /// The segment that this entry was recovered from.
    #[must_use]
    pub fn segment(&self) -> &RecoveredSegment {
        &self.reader.header
    }

    /// Reads the next chunk of data written in this entry. If another chunk of
    /// data is found in this entry, [`ReadChunkResult::Chunk`] will be
    /// returned. If no other chunks are found, [`ReadChunkResult::EndOfEntry`]
    /// will be returned.
    ///
    /// # Aborted Log Entries
    ///
    /// In the event of recovering from a crash or power outage, it is possible
    /// to receive [`ReadChunkResult::AbortedEntry`]. It is also possible when
    /// reading from a returned [`EntryChunk`] to encounter an unexpected
    /// end-of-file error. When these situations arise, the entire entry that
    /// was being read should be ignored.
    ///
    /// The format chosen for this log format makes some tradeoffs, and one of
    /// the tradeoffs is not knowing the full length of an entry when it is
    /// being written. This allows for very large log entries to be written
    /// without requiring memory for the entire entry.
    pub fn read_chunk<'chunk>(&'chunk mut self) -> io::Result<ReadChunkResult<'chunk, 'entry>> {
        if self.reader.file.buffer().is_empty() {
            self.reader.file.fill_buf()?;
        }

        match self.reader.file.buffer().first().copied() {
            Some(CHUNK) => {
                let mut header_bytes = [0; 5];
                let offset = self.reader.file.stream_position()?;
                self.reader.file.read_exact(&mut header_bytes)?;
                Ok(ReadChunkResult::Chunk(EntryChunk {
                    position: LogPosition {
                        file_id: self.reader.file_id,
                        offset,
                    },
                    entry: self,
                    calculated_crc: 0,
                    stored_crc32: None,
                    bytes_remaining: u32::from_le_bytes(
                        header_bytes[1..5].try_into().expect("u32 is 4 bytes"),
                    ),
                }))
            }
            Some(END_OF_ENTRY) => {
                self.reader.file.consume(1);
                Ok(ReadChunkResult::EndOfEntry)
            }
            _ => Ok(ReadChunkResult::AbortedEntry),
        }
    }

    /// Reads all chunks for this entry. If the entry was completely written,
    /// the list of chunks of data is returned. If the entry wasn't completely
    /// written, None will be returned.
    pub fn read_all_chunks(&mut self) -> io::Result<Option<Vec<Vec<u8>>>> {
        let mut chunks = Vec::new();
        loop {
            let mut chunk = match self.read_chunk()? {
                ReadChunkResult::Chunk(chunk) => chunk,
                ReadChunkResult::EndOfEntry => break,
                ReadChunkResult::AbortedEntry => return Ok(None),
            };
            chunks.push(chunk.read_all()?);
            if !chunk.check_crc()? {
                return Err(io::Error::new(ErrorKind::InvalidData, "crc check failed"));
            }
        }
        Ok(Some(chunks))
    }
}

/// The result of reading a chunk from a log segment.
#[derive(Debug)]
pub enum ReadChunkResult<'chunk, 'entry> {
    /// A chunk was found.
    Chunk(EntryChunk<'chunk, 'entry>),
    /// The end of the entry has been reached.
    EndOfEntry,
    /// An aborted entry was detected. This should only be encountered if log
    /// entries were being written when the computer or application crashed.
    ///
    /// When is returned, the entire entry should be ignored.
    AbortedEntry,
}

/// A chunk of data previously written using
/// [`EntryWriter::write_chunk`](crate::EntryWriter::write_chunk).
///
/// # Panics
///
/// Once dropped, this type will ensure that the entry reader is advanced to the
/// end of this chunk if needed by calling
/// [`EntryChunk::skip_remaining_bytes()`]. If an error occurs during this call,
/// a panic will occur.
///
/// To prevent all possibilities of panics, all bytes should be exhausted before
/// dropping this type by:
///
/// - Using [`Read`] until a 0 is returned.
/// - Using [`EntryChunk::read_all()`] to read all remaining bytes at once.
/// - Skipping all remaining bytes using [`EntryChunk::skip_remaining_bytes()`]
#[derive(Debug)]
pub struct EntryChunk<'chunk, 'entry> {
    entry: &'chunk mut Entry<'entry>,
    position: LogPosition,
    bytes_remaining: u32,
    calculated_crc: u32,
    stored_crc32: Option<u32>,
}

impl<'chunk, 'entry> EntryChunk<'chunk, 'entry> {
    /// Returns the position that this chunk is located at.
    #[must_use]
    pub fn log_position(&self) -> LogPosition {
        self.position
    }

    /// Returns the number of bytes remaining to read from this chunk.
    #[must_use]
    pub const fn bytes_remaining(&self) -> u32 {
        self.bytes_remaining
    }

    /// Returns true if the CRC has been validated, or false if the computed crc
    /// is different than the stored crc. Returns an error if the chunk has not
    /// been fully read yet.
    pub fn check_crc(&mut self) -> io::Result<bool> {
        if self.bytes_remaining == 0 {
            if self.stored_crc32.is_none() {
                let mut stored_crc32 = [0; 4];
                // Bypass our internal read, otherwise our crc would include the
                // crc read itself.
                self.entry.reader.file.read_exact(&mut stored_crc32)?;
                self.stored_crc32 = Some(u32::from_le_bytes(stored_crc32));
            }

            Ok(self.stored_crc32.expect("already initialized") == self.calculated_crc)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "crc cannot be checked before reading all chunk bytes",
            ))
        }
    }

    /// Reads all of the remaining data from this chunk.
    pub fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut data = Vec::with_capacity(usize::try_from(self.bytes_remaining).to_io()?);
        self.read_to_end(&mut data)?;
        Ok(data)
    }

    /// Advances past the end of this chunk without reading the remaining bytes.
    pub fn skip_remaining_bytes(mut self) -> io::Result<()> {
        self.skip_remaining_bytes_internal()
    }

    /// Advances past the end of this chunk without reading the remaining bytes.
    fn skip_remaining_bytes_internal(&mut self) -> io::Result<()> {
        if self.bytes_remaining > 0 || self.stored_crc32.is_none() {
            // Skip past the remaining bytes plus the crc.
            self.entry
                .reader
                .file
                .seek(SeekFrom::Current(i64::from(self.bytes_remaining + 4)))?;
            self.bytes_remaining = 0;
        }
        Ok(())
    }
}

impl<'chunk, 'entry> Read for EntryChunk<'chunk, 'entry> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_remaining = usize::try_from(self.bytes_remaining).to_io()?;
        let bytes_to_read = bytes_remaining.min(buf.len());

        if bytes_to_read > 0 {
            let bytes_read = self.entry.reader.file.read(&mut buf[..bytes_to_read])?;
            self.bytes_remaining -= u32::try_from(bytes_read).to_io()?;
            self.calculated_crc = crc32c::crc32c_append(self.calculated_crc, &buf[..bytes_read]);
            Ok(bytes_read)
        } else {
            Ok(0)
        }
    }
}

impl<'chunk, 'entry> Drop for EntryChunk<'chunk, 'entry> {
    fn drop(&mut self) {
        self.skip_remaining_bytes_internal()
            .expect("error while skipping remaining bytes");
    }
}

/// Information about an individual segment of a
/// [`WriteAheadLog`](crate::WriteAheadLog) that is being recovered.
#[derive(Debug)]
pub struct RecoveredSegment {
    /// The value of [`Configuration::version_info`] at the time this segment
    /// was created.
    pub version_info: Vec<u8>,
}
