#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms
)]
#![allow(
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc
)]

mod file;

use std::{
    collections::VecDeque,
    fmt::Debug,
    fs::OpenOptions,
    io::{self, BufRead, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    num::TryFromIntError,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
};

use file::File;
use parking_lot::{Condvar, Mutex};
use watchable::{Watchable, Watcher};

use crate::file::{AnyFileKind, FileManager, LockedFile};

/// A Write-Ahead Log that enables atomic, durable writes.
#[derive(Debug, Clone)]
pub struct WriteAheadLog {
    data: Arc<Data>,
}

impl WriteAheadLog {
    /// Creates or recovers a log stored in `directory` using the default
    /// [`Config`].
    ///
    /// # Errors
    ///
    /// Returns various errors if the log is unable to be successfully
    /// recovered.
    pub fn recover(
        directory: impl AsRef<Path>,
        checkpointer: impl Checkpointer + 'static,
    ) -> io::Result<Self> {
        Self::recover_with_config(directory, checkpointer, Configuration::default())
    }

    /// Creates or recovers a log stored in `directory` using the provided
    /// configuration.
    ///
    /// # Errors
    ///
    /// Returns various errors if the log is unable to be successfully
    /// recovered.
    pub fn recover_with_config(
        directory: impl AsRef<Path>,
        mut checkpointer: impl Checkpointer + 'static,
        config: Configuration,
    ) -> io::Result<Self> {
        config.validate()?;
        let directory = directory.as_ref().to_path_buf();

        let mut segments = SegmentFiles::default();
        if directory.exists() {
            for id in 0..=u16::MAX {
                let path = directory.join(format!("wal{id}"));

                if config.file_manager.path_exists(&path) {
                    let file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .read(true)
                        .open(&path)?;

                    let length = file.metadata()?.len();
                    segments.slots.push(FileSlot {
                        path: Arc::new(path),
                        first_entry_id: None,
                        latest_entry_id: None,
                        writer: Some(LockedFile::new(File {
                            length: Some(length),
                            position: 0,
                            file: AnyFileKind::Std { file },
                        })),
                        active: false,
                    });
                } else {
                    break;
                }
            }
        } else {
            config.file_manager.create_dir_recursive(&directory)?;
        }

        let latest_entry_id = Self::recover_files(&mut segments, &mut checkpointer, &config)?;

        segments.segment_sync_index =
            u16::try_from(segments.slots.len().saturating_sub(1)).to_io()?;

        let watchable = Watchable::new(EntryId(0));
        let watcher = watchable.watch();
        let wal = Self {
            data: Arc::new(Data {
                next_entry_id: AtomicU64::new(latest_entry_id.0 + 1),
                directory,
                config,
                checkpointer: Mutex::new(Box::new(checkpointer)),
                segments: Mutex::new(segments),
                segments_sync: Condvar::new(),
                dirsync_sync: Condvar::new(),
                checkpoint_sync: Condvar::new(),
                checkpoint_to_sender: watchable,
            }),
        };
        let thread_wal = Arc::downgrade(&wal.data);
        std::thread::Builder::new()
            .name(String::from("okaywal-cp"))
            .spawn(move || Self::checkpoint_loop(&thread_wal, watcher))
            .expect("thread spawn failed");

        Ok(wal)
    }

    /// Opens the log for writing. The entry written by the returned
    /// [`EntryWriter`] is ordered in the log at the time of creation and will be
    /// recovered in the same order.
    ///
    /// # Errors
    ///
    /// Returns various errors that may occur while acquiring a log segment to
    /// write into.
    pub fn write(&self) -> io::Result<EntryWriter> {
        let mut segments = self.data.segments.lock();

        loop {
            if let Some(slot_id) = segments.active_slots.pop_front() {
                let file = segments.slots[usize::from(slot_id)]
                    .writer
                    .take()
                    .expect("missing file");

                return EntryWriter::new(self.clone(), slot_id, file);
            } else if segments.active_slot_count < self.data.config.active_segment_limit {
                if let Some((slot_id, slot)) = segments
                    .slots
                    .iter_mut()
                    .enumerate()
                    .find(|(_, slot)| !slot.active)
                {
                    let file = slot.writer.take().expect("missing file");
                    slot.active = true;
                    segments.active_slot_count += 1;

                    return EntryWriter::new(self.clone(), u16::try_from(slot_id).to_io()?, file);
                }

                // Open a new segment file
                segments.active_slot_count += 1;

                let slot_id = segments.slots.len();
                let path = self.data.directory.join(format!("wal{slot_id}"));
                let mut file =
                    LockedFile::new(self.data.config.file_manager.append_to_path(&path)?);

                self.data.config.write_segment_header(&mut file)?;

                segments.slots.push(FileSlot {
                    path: Arc::new(path),
                    first_entry_id: None,
                    latest_entry_id: None,
                    writer: None,
                    active: true,
                });

                return EntryWriter::new(self.clone(), u16::try_from(slot_id).to_io()?, file);
            }

            self.data.segments_sync.wait(&mut segments);
        }
    }

    /// Opens the log to read previously written data.
    ///
    /// # Errors
    ///
    /// May error if:
    ///
    /// - The file cannot be read.
    /// - The position refers to data that has been checkpointed.
    pub fn read_at(&self, position: LogPosition) -> io::Result<File> {
        let segments = self.data.segments.lock();
        let path = segments.slots[usize::from(position.slot_id)].path.clone();
        drop(segments);

        let mut file = self.data.config.file_manager.read_path(&path)?;
        file.seek(SeekFrom::Start(position.offset + 9))?;
        Ok(file)
    }

    fn reclaim(
        &self,
        slot_id: u16,
        file: LockedFile,
        written_entry: Option<EntryId>,
        bytes_written: Option<u64>,
    ) -> io::Result<()> {
        let mut segments = self.data.segments.lock();

        let slot_index = usize::from(slot_id);

        if slot_id > segments.segment_sync_index {
            loop {
                if slot_id <= segments.segment_sync_index {
                    break;
                } else if segments.directory_is_syncing {
                    // Wait on another thread to finish syncing.
                    self.data.dirsync_sync.wait(&mut segments);
                } else {
                    // Become the directory sync thread
                    segments.directory_is_syncing = true;
                    drop(segments);

                    // Synchronize now that threads are unblocked
                    self.data
                        .config
                        .file_manager
                        .synchronize_path(&self.data.directory)?;

                    // Reaquire the lock and update the state.
                    segments = self.data.segments.lock();
                    segments.segment_sync_index = slot_id;
                    segments.directory_is_syncing = false;
                    self.data.dirsync_sync.notify_all();
                    break;
                }
            }
        }

        segments.slots[slot_index].writer = Some(file);
        if let (Some(entry_id), Some(bytes_written)) = (written_entry, bytes_written) {
            if segments.slots[slot_index].first_entry_id.is_none() {
                segments.slots[slot_index].first_entry_id = Some(entry_id);
            }
            segments.slots[slot_index].latest_entry_id = Some(entry_id);

            segments.uncheckpointed_bytes += bytes_written;
            if segments.uncheckpointed_bytes >= self.data.config.checkpoint_after_bytes
                && self.data.checkpoint_to_sender.update(entry_id).is_ok()
            {
                segments.uncheckpointed_bytes = 0;
                segments.remove_segments_for_archiving(entry_id);
            }
        }

        let checkpoint_to = *self.data.checkpoint_to_sender.write();
        let segment_made_available = if segments.slots[slot_index].first_entry_id.is_some()
            && segments.slots[slot_index].first_entry_id.unwrap() <= checkpoint_to
        {
            segments.slots[slot_index].active = false;
            segments.active_slot_count -= 1;
            let latest_id = segments.slots[slot_index]
                .latest_entry_id
                .expect("always present if first_entry_id is");
            if checkpoint_to < latest_id {
                self.data.checkpoint_to_sender.replace(latest_id);

                segments.remove_segments_for_archiving(checkpoint_to);
            }
            false
        } else {
            segments.active_slots.push_back(slot_id);
            true
        };

        drop(segments);

        if segment_made_available {
            self.data.segments_sync.notify_one();
        } else {
            self.data.checkpoint_sync.notify_one();
        }

        Ok(())
    }

    fn recover_files<A: Checkpointer>(
        files: &mut SegmentFiles,
        checkpointer: &mut A,
        config: &Configuration,
    ) -> io::Result<EntryId> {
        let mut readers = Vec::with_capacity(files.slots.len());
        let mut completed_readers = Vec::with_capacity(files.slots.len());
        let mut latest_entry_id = 0;
        for (index, slot) in files.slots.iter().enumerate() {
            let file = config.file_manager.read_path(&slot.path)?;
            let reader = SegmentReader::new(index, file)?;
            match checkpointer.should_recover_segment(&reader.header)? {
                Recovery::Recover => {
                    readers.push(reader);
                }
                Recovery::Abandon => {
                    completed_readers.push(reader);
                }
            }
        }

        while !readers.is_empty() {
            // Find the next entry id in sequence across all remaining readers.
            // If a reader has no more entries, remove it from consideration.
            let mut lowest_entry_id_remaining = u64::MAX;
            let mut lowest_entry_reader = None;
            let mut index = 0;
            while index < readers.len() {
                if let Some(entry_id) = readers[index].current_entry_id() {
                    if lowest_entry_reader.is_none() || entry_id.0 < lowest_entry_id_remaining {
                        lowest_entry_reader = Some(index);
                        lowest_entry_id_remaining = entry_id.0;
                    }
                    index += 1;
                } else {
                    // Reader is exhausted
                    let reader = readers.remove(index);
                    completed_readers.push(reader);
                }
            }

            if let Some(lowest_entry_reader) = lowest_entry_reader {
                let reader = &mut readers[lowest_entry_reader];

                latest_entry_id = reader.current_entry_id.unwrap().0;
                let mut entry = Entry {
                    id: reader.current_entry_id.unwrap(),
                    reader,
                };
                checkpointer.recover(&mut entry)?;
                while let Some(mut chunk) = match entry.read_chunk()? {
                    ReadChunkResult::Chunk(chunk) => Some(chunk),
                    _ => None,
                } {
                    // skip chunk
                    chunk.skip_remaining_bytes()?;
                }

                if !reader.read_next_entry()? {
                    // No more entries.
                    readers.remove(lowest_entry_reader);
                }
            }
        }

        // We've now informed the checkpointer of all commits, we need to clean up
        // the files (truncating any invalid data) and enqueue them.
        for reader in completed_readers {
            let slot = &mut files.slots[reader.slot_index];
            slot.first_entry_id = reader.first_entry_id;
            slot.latest_entry_id = reader.last_entry_id;
            let writer = slot.writer.as_mut().unwrap();
            if slot.first_entry_id.is_none() {
                writer.set_len(0)?;
                writer.seek(SeekFrom::Start(0))?;
                config.write_segment_header(writer)?;
            } else if writer.len()? != reader.valid_until {
                writer.set_len(reader.valid_until)?;
            }

            // bytes_per_page is compatible, we can continue writing to
            // this slot.
            files
                .active_slots
                .push_back(u16::try_from(reader.slot_index).unwrap());
            files.active_slot_count += 1;
        }

        Ok(EntryId(latest_entry_id))
    }

    fn checkpoint_loop(data: &Weak<Data>, mut checkpoint_to: Watcher<EntryId>) {
        for mut entry_to_checkpoint_to in &mut checkpoint_to {
            let data = if let Some(data) = data.upgrade() {
                data
            } else {
                // The final instance was dropped before we could checkpoint.
                break;
            };

            let mut segments = data.segments.lock();
            // Wait for all outstanding files that have an EntryId that is <=
            // entry_to_checkpoint_to
            while !segments
                .slots
                .iter()
                .all(|slot| slot.can_checkpoint_if_needed(entry_to_checkpoint_to))
            {
                data.checkpoint_sync.wait(&mut segments);
                // Other files may have been returned that need to be
                // checkpointed but have a later entry id.
                entry_to_checkpoint_to = *data.checkpoint_to_sender.write();
            }

            let mut checkpointer = data.checkpointer.lock();
            // TODO error handling?
            checkpointer.checkpoint_to(entry_to_checkpoint_to).unwrap();

            for slot in segments
                .slots
                .iter_mut()
                .filter(|slot| slot.should_checkpoint(entry_to_checkpoint_to))
            {
                let writer = slot.writer.as_mut().unwrap();
                // TODO error handling?
                writer.set_len(0).unwrap();
                writer.seek(SeekFrom::Start(0)).unwrap();
                data.config.write_segment_header(writer).unwrap();
                slot.first_entry_id = None;
                slot.latest_entry_id = None;
            }
        }
    }
}

#[derive(Debug)]
struct Data {
    next_entry_id: AtomicU64,
    directory: PathBuf,
    checkpointer: Mutex<Box<dyn Checkpointer>>,
    segments: Mutex<SegmentFiles>,
    segments_sync: Condvar,
    dirsync_sync: Condvar,
    checkpoint_sync: Condvar,
    checkpoint_to_sender: Watchable<EntryId>,
    config: Configuration,
}

/// Controls a [`WriteAheadLog`]'s behavior.
#[derive(Debug)]
pub struct Configuration {
    /// This information is written to the start of each log segment, and is
    /// available for reading during [`Checkpointer::should_recover`]. This
    /// field is intended to be used to identify the format of the data stored
    /// within the log, allowing for the format to change over time without
    /// breaking the abilty to recover existing log files during an upgrade.
    pub version_info: Vec<u8>,
    /// The maximum number of segments that can have a [`EntryWriter`] at any
    /// given time.
    pub active_segment_limit: usize,
    /// The number of bytes that need to be written before checkpointing will
    /// automatically occur. The actual amount of data checkpointed may be
    /// significantly higher than this, depending on all in-progress writes at
    /// the time the threshold is surpassed.
    ///
    /// Only one checkpoint operation can happen at any given time. This means
    /// that if data is written faster to the log than it can be checkpointed,
    /// the WAL may grow significantly larger than this threshold.
    pub checkpoint_after_bytes: u64,
    /// The file manager to use for all file operations.
    pub file_manager: FileManager,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            version_info: Vec::default(),
            active_segment_limit: 8,
            checkpoint_after_bytes: 8 * 1024 * 1024,
            file_manager: FileManager::Std,
        }
    }
}

impl Configuration {
    fn validate(&self) -> io::Result<()> {
        if self.version_info.len() > 255 {
            return Err(io::Error::new(
                ErrorKind::Other,
                "version_info must be <= 255 bytes",
            ));
        }

        Ok(())
    }

    fn write_segment_header(&self, file: &mut File) -> io::Result<()> {
        let mut buf = BufWriter::new(file);
        buf.write_all(b"okw\0")?;
        let version_size = u8::try_from(self.version_info.len()).to_io()?;
        buf.write_all(&[version_size])?;
        buf.write_all(&self.version_info)?;
        buf.flush()?;

        Ok(())
    }
}

/// Customizes recovery and checkpointing behavior for a [`WriteAheadLog`].
pub trait Checkpointer: Send + Sync + Debug {
    /// When recovering a [`WriteAheadLog`], this function is called for each
    /// segment as it is read. To allow the segment to have its data recovered,
    /// return [`Recovery::Recover`]. If you wish to abandon the data contained
    /// in the segment, return [`Recovery::Abandon`].
    fn should_recover_segment(&mut self, segment: &RecoveredSegment) -> io::Result<Recovery>;
    /// Invoked once for each entry contained in all recovered segments within a
    /// [`WriteAheadLog`].
    ///
    /// [`Entry::read_chunk()`] can be used to read each chunk of data that was
    /// written via [`EntryWriter::write_chunk`]. The order of chunks is guaranteed
    /// to be the same as the order they were written in.
    fn recover(&mut self, entry: &mut Entry<'_>) -> io::Result<()>;

    /// Invoked each time the [`WriteAheadLog`] is ready to recycle and reuse
    /// segment files.
    fn checkpoint_to(&mut self, last_checkpointed_id: EntryId) -> io::Result<()>;
}

/// Information about an individual segment of a [`WriteAheadLog`] that is being
/// recovered.
#[derive(Debug)]
pub struct RecoveredSegment {
    /// The value of [`Configuration::version_info`] at the time this segment
    /// was created.
    pub version_info: Vec<u8>,
}

/// The unique id of an entry written to a [`WriteAheadLog`]. These IDs are
/// ordered by the time the [`EntryWriter`] was created for the entry written with this id.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default)]
pub struct EntryId(pub u64);

/// A stored entry inside of a [`WriteAheadLog`].
///
/// Each entry is composed of a series of chunks of data that were previously
/// written using [`EntryWriter::write_chunk`].
#[derive(Debug)]
pub struct Entry<'a> {
    id: EntryId,
    reader: &'a mut SegmentReader,
}

impl<'entry> Entry<'entry> {
    /// The unique id of this entry.
    #[must_use]
    pub const fn id(&self) -> EntryId {
        self.id
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
        if self.reader.file.buffer().len() < 9 {
            self.reader.file.fill_buf()?;
        }

        if self.reader.file.buffer().len() > 9 && self.reader.file.buffer()[0] == CHUNK {
            let mut header_bytes = [0; 9];
            self.reader.file.read_exact(&mut header_bytes)?;
            Ok(ReadChunkResult::Chunk(EntryChunk {
                entry: self,
                calculated_crc: 0,
                stored_crc: u32::from_le_bytes(
                    header_bytes[1..5].try_into().expect("u32 is 4 bytes"),
                ),
                bytes_remaining: u32::from_le_bytes(
                    header_bytes[5..].try_into().expect("u32 is 4 bytes"),
                ),
            }))
        } else if self.reader.file.buffer().first().copied() == Some(END_OF_ENTRY) {
            self.reader.file.consume(1);
            Ok(ReadChunkResult::EndOfEntry)
        } else {
            Ok(ReadChunkResult::AbortedEntry)
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

/// A chunk of data previously written using [`EntryWriter::write_chunk`].
#[derive(Debug)]
pub struct EntryChunk<'chunk, 'entry> {
    entry: &'chunk mut Entry<'entry>,
    bytes_remaining: u32,
    stored_crc: u32,
    calculated_crc: u32,
}

impl<'chunk, 'entry> EntryChunk<'chunk, 'entry> {
    /// Returns the number of bytes remaining to read from this chunk.
    #[must_use]
    pub const fn bytes_remaining(&self) -> u32 {
        self.bytes_remaining
    }

    /// Returns true if the CRC has been validated, or false if the computed crc
    /// is different than the stored crc. Returns `None` if not all data has
    /// been read yet.
    #[must_use]
    pub const fn check_crc(&self) -> Option<bool> {
        if self.bytes_remaining > 0 {
            None
        } else {
            Some(self.stored_crc == self.calculated_crc)
        }
    }

    /// Reads all of the remaining data from this chunk.
    pub fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut data = Vec::with_capacity(usize::try_from(self.bytes_remaining).to_io()?);
        self.read_to_end(&mut data)?;
        Ok(data)
    }

    fn skip_remaining_bytes(&mut self) -> io::Result<()> {
        if self.bytes_remaining > 0 {
            self.entry
                .reader
                .file
                .seek(SeekFrom::Current(i64::from(self.bytes_remaining)))?;
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

/// Determines whether to recover a segment or not.
pub enum Recovery {
    /// Recover the segment.
    Recover,
    /// Abandon the segment and any entries stored within it. **Warning: This
    /// means losing data that was previously written to the log. This should
    /// rarely, if ever, be done.**
    Abandon,
}

#[derive(Debug, Default)]
struct SegmentFiles {
    directory_is_syncing: bool,
    segment_sync_index: u16,
    slots: Vec<FileSlot>,
    active_slots: VecDeque<u16>,
    active_slot_count: usize,
    uncheckpointed_bytes: u64,
}

impl SegmentFiles {
    fn remove_segments_for_archiving(&mut self, checkpoint_to: EntryId) {
        let mut active_slot_index = 0;
        while active_slot_index < self.active_slots.len() {
            let slot = &mut self.slots[usize::from(self.active_slots[active_slot_index])];
            if slot.first_entry_id.is_some() && slot.first_entry_id.unwrap() <= checkpoint_to {
                slot.active = false;
                // This segment contains data that needs to be checkpointed.
                self.active_slots.remove(active_slot_index);
                self.active_slot_count -= 1;
            } else {
                active_slot_index += 1;
            }
        }
    }
}

#[derive(Debug)]
struct FileSlot {
    path: Arc<PathBuf>,
    first_entry_id: Option<EntryId>,
    latest_entry_id: Option<EntryId>,
    writer: Option<LockedFile>,
    active: bool,
}

impl FileSlot {
    fn should_checkpoint(&self, checkpoint_to: EntryId) -> bool {
        self.first_entry_id.is_some() && self.first_entry_id.unwrap() <= checkpoint_to
    }

    fn can_checkpoint_if_needed(&self, checkpoint_to: EntryId) -> bool {
        let should_checkpoint = self.should_checkpoint(checkpoint_to);
        !should_checkpoint || self.writer.is_some()
    }
}

/// Writes an entry to a [`WriteAheadLog`].
///
/// Each entry has a unique id and one or more chunks of data.
pub struct EntryWriter {
    /// The unique id of the entry being written.
    pub entry_id: EntryId,
    log: WriteAheadLog,
    slot_id: u16,
    file: Option<BufWriter<LockedFile>>,
    original_file_length: u64,
    bytes_written: u64,
}

const NEW_ENTRY: u8 = 1;
const CHUNK: u8 = 2;
const END_OF_ENTRY: u8 = 3;

impl EntryWriter {
    fn new(log: WriteAheadLog, slot_id: u16, file: LockedFile) -> io::Result<Self> {
        let entry_id = EntryId(log.data.next_entry_id.fetch_add(1, Ordering::SeqCst));
        let original_file_length = file.len()?;
        let mut file = BufWriter::new(file);

        file.write_all(&[NEW_ENTRY])?;
        file.write_all(&entry_id.0.to_le_bytes())?;

        Ok(Self {
            entry_id,
            log,
            slot_id,
            file: Some(file),
            original_file_length,
            bytes_written: 9, // entry header
        })
    }

    /// Commits this entry to the log. Once this call returns, all data is
    /// atomically updated and synchronized to disk.
    pub fn commit(self) -> io::Result<EntryId> {
        self.commit_and(|_file| Ok(()))
    }

    fn commit_and<F: FnOnce(&mut BufWriter<LockedFile>) -> io::Result<()>>(
        mut self,
        callback: F,
    ) -> io::Result<EntryId> {
        let mut file = self.file.take().expect("already committed");
        file.write_all(&[END_OF_ENTRY])?;
        callback(&mut file)?;
        let mut file = file.into_inner()?;
        file.sync_data()?;

        self.log.reclaim(
            self.slot_id,
            file,
            Some(self.entry_id),
            Some(self.bytes_written),
        )?;

        Ok(self.entry_id)
    }

    /// Appends a chunk of data to this log entry. Each chunk of data is able to
    /// be read using [`Entry::read_chunk`].
    pub fn write_chunk(&mut self, data: &[u8]) -> io::Result<ChunkRecord> {
        let length = u32::try_from(data.len()).to_io()?;
        let position = self.position()?;
        let file = self.file.as_mut().expect("already dropped");
        let file_length = file.get_ref().len()? + u64::try_from(file.buffer().len()).to_io()?;
        if let Some(space_needed) =
            (position.offset + u64::from(length) + 9).checked_sub(file_length)
        {
            let minimum_length = space_needed + file_length;
            // Need more disk space
            let space_to_allocate = (minimum_length / 16).max(64 * 1024);
            let new_length = file_length + space_to_allocate;
            // println!("More space {space_to_allocate} - {space_needed} = {new_length}");
            file.flush()?;
            file.get_mut().set_len(new_length)?;
            file.seek(SeekFrom::Start(position.offset))?;
        }
        let crc = crc32c::crc32c(data);
        let mut header = [CHUNK; 9];
        header[1..5].copy_from_slice(&crc.to_le_bytes());
        header[5..].copy_from_slice(&length.to_le_bytes());
        file.write_all(&header)?;
        file.write_all(data)?;

        self.bytes_written += 9 + u64::from(length);

        Ok(ChunkRecord {
            position,
            crc,
            length,
        })
    }

    fn position(&self) -> io::Result<LogPosition> {
        let file = self.file.as_ref().expect("already dropped");
        let offset = file.get_ref().position();
        let pending_bytes = u64::try_from(file.buffer().len()).to_io()?;
        Ok(LogPosition {
            slot_id: self.slot_id,
            offset: offset + pending_bytes,
        })
    }

    /// Abandons this entry, preventing the entry from being recovered in the
    /// future. This is automatically done when dropped, but errors that occur
    /// during drop will panic.
    pub fn abandon(mut self) -> io::Result<()> {
        self.rollback_session()
    }

    fn rollback_session(&mut self) -> io::Result<()> {
        let mut file = self
            .file
            .take()
            .expect("file already dropped")
            .into_inner()?;

        file.set_len(self.original_file_length)?;

        self.log.reclaim(self.slot_id, file, None, None)?;

        Ok(())
    }
}

impl Drop for EntryWriter {
    fn drop(&mut self) {
        if self.file.is_some() {
            self.rollback_session().unwrap();
        }
    }
}

/// The position of a chunk of data within a [`WriteAheadLog`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct LogPosition {
    slot_id: u16,
    offset: u64,
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

trait ToIoResult<T> {
    fn to_io(self) -> io::Result<T>;
}

impl<T> ToIoResult<T> for Result<T, TryFromIntError> {
    fn to_io(self) -> io::Result<T> {
        match self {
            Ok(result) => Ok(result),
            Err(err) => Err(io::Error::new(ErrorKind::Other, err)),
        }
    }
}

#[derive(Debug)]
struct SegmentReader {
    slot_index: usize,
    file: BufReader<File>,
    header: RecoveredSegment,
    current_entry_id: Option<EntryId>,
    first_entry_id: Option<EntryId>,
    last_entry_id: Option<EntryId>,
    valid_until: u64,
}

impl SegmentReader {
    pub fn new(slot_id: usize, file: File) -> io::Result<Self> {
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

        let mut segment_reader = Self {
            slot_index: slot_id,
            file,
            header,
            current_entry_id: None,
            first_entry_id: None,
            last_entry_id: None,
            valid_until: u64::from(version_info_length) + 5,
        };

        segment_reader.read_next_entry()?;

        Ok(segment_reader)
    }

    pub fn current_entry_id(&mut self) -> Option<EntryId> {
        self.current_entry_id
    }

    fn read_next_entry(&mut self) -> io::Result<bool> {
        if self.file.buffer().is_empty() {
            self.file.fill_buf()?;
        }

        let mut header_bytes = [0; 9];
        match self.file.read_exact(&mut header_bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(false),
            Err(err) => return Err(err),
        }

        match header_bytes[0] {
            NEW_ENTRY => {
                self.current_entry_id = Some(EntryId(u64::from_le_bytes(
                    header_bytes[1..].try_into().unwrap(),
                )));

                Ok(true)
            }
            0 => {
                self.current_entry_id = None;
                Ok(false)
            }
            other => Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid entry byte: {other}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests;

/// A [`Checkpointer`] that does not attempt to recover any existing data.
#[derive(Debug)]
pub struct VoidCheckpointer;

impl Checkpointer for VoidCheckpointer {
    fn should_recover_segment(&mut self, _segment: &RecoveredSegment) -> io::Result<Recovery> {
        Ok(Recovery::Abandon)
    }

    fn recover(&mut self, _entry: &mut Entry<'_>) -> io::Result<()> {
        Ok(())
    }

    fn checkpoint_to(&mut self, _last_checkpointed_id: EntryId) -> io::Result<()> {
        Ok(())
    }
}
