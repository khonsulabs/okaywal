#![doc = include_str!(".crate-docs.md")]
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

use std::{
    collections::{HashMap, VecDeque},
    ffi::OsStr,
    io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom},
    path::Path,
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::Instant,
};

use file_manager::{fs::StdFileManager, FileManager, OpenOptions, PathId};
use parking_lot::{Condvar, Mutex, MutexGuard};

pub use crate::{
    config::Configuration,
    entry::{ChunkRecord, EntryId, EntryWriter, LogPosition},
    log_file::{Entry, EntryChunk, ReadChunkResult, RecoveredSegment, SegmentReader},
    manager::{LogManager, LogVoid, Recovery},
};
use crate::{log_file::LogFile, to_io_result::ToIoResult};
pub use file_manager;

mod buffered;
mod config;
mod entry;
mod log_file;
mod manager;
mod to_io_result;

/// A [Write-Ahead Log][wal] that provides atomic and durable writes.
///
/// This type implements [`Clone`] and can be passed between multiple threads
/// safely.
///
/// When [`WriteAheadLog::begin_entry()`] is called, an exclusive lock to the
/// active log segment is acquired. The lock is released when the entry is
/// [rolled back](entry::EntryWriter::rollback) or once the entry is in queue
/// for synchronization during
/// [`EntryWriter::commit()`](entry::EntryWriter::commit).
///
/// Multiple threads may be queued for synchronization at any given time. This
/// allows good multi-threaded performance in spite of only using a single
/// active file.
///
/// [wal]: https://en.wikipedia.org/wiki/Write-ahead_logging
#[derive(Debug, Clone)]
pub struct WriteAheadLog<M = StdFileManager>
where
    M: FileManager,
{
    data: Arc<Data<M>>,
}

#[derive(Debug)]
struct Data<M>
where
    M: FileManager,
{
    files: Mutex<Files<M::File>>,
    manager: Mutex<Box<dyn LogManager<M>>>,
    active_sync: Condvar,
    dirfsync_sync: Condvar,
    config: Configuration<M>,
    checkpoint_sender: flume::Sender<CheckpointCommand<M::File>>,
    checkpoint_thread: Mutex<Option<JoinHandle<io::Result<()>>>>,
    readers: Mutex<HashMap<u64, usize>>,
    readers_sync: Condvar,
}

impl WriteAheadLog<StdFileManager> {
    /// Creates or opens a log in `directory` using the provided log manager to
    /// drive recovery and checkpointing.
    pub fn recover<AsRefPath: AsRef<Path>, Manager: LogManager>(
        directory: AsRefPath,
        manager: Manager,
    ) -> io::Result<Self> {
        Configuration::default_for(directory).open(manager)
    }
}
impl<M> WriteAheadLog<M>
where
    M: FileManager,
{
    fn open<Manager: LogManager<M>>(
        config: Configuration<M>,
        mut manager: Manager,
    ) -> io::Result<Self> {
        if !config.file_manager.exists(&config.directory) {
            config.file_manager.create_dir_all(&config.directory)?;
        }

        // Find all files that match this pattern: "wal-[0-9]+(-cp)?"
        let mut discovered_files = Vec::new();
        for file in config.file_manager.list(&config.directory)? {
            if let Some(file_name) = file.file_name().and_then(OsStr::to_str) {
                let mut parts = file_name.split('-');
                let prefix = parts.next();
                let entry_id = parts.next().and_then(|ts| ts.parse::<u64>().ok());
                let suffix = parts.next();
                match (prefix, entry_id, suffix) {
                    (Some(prefix), Some(entry_id), suffix) if prefix == "wal" => {
                        let has_checkpointed = suffix == Some("cp");
                        discovered_files.push((entry_id, file, has_checkpointed));
                    }
                    _ => {}
                }
            }
        }

        // Sort by the timestamp
        discovered_files.sort_by(|a, b| a.0.cmp(&b.0));

        let mut files = Files::default();
        let mut files_to_checkpoint = Vec::new();
        for (entry_id, path, has_checkpointed) in discovered_files {
            // We can safely assume that the entry id prior to the one that
            // labels the file have been used.
            files.last_entry_id = EntryId(entry_id - 1);
            if has_checkpointed {
                let file = LogFile::write(entry_id, path, 0, None, &config)?;
                files.all.insert(entry_id, file.clone());
                files.inactive.push_back(file);
            } else {
                let mut reader = SegmentReader::new(&path, entry_id, &config.file_manager)?;
                match manager.should_recover_segment(&reader.header)? {
                    Recovery::Recover => {
                        while let Some(mut entry) = reader.read_entry()? {
                            manager.recover(&mut entry)?;
                            while let Some(chunk) = match entry.read_chunk()? {
                                ReadChunkResult::Chunk(chunk) => Some(chunk),
                                ReadChunkResult::EndOfEntry | ReadChunkResult::AbortedEntry => None,
                            } {
                                chunk.skip_remaining_bytes()?;
                            }
                            files.last_entry_id = entry.id();
                        }

                        let file = LogFile::write(
                            entry_id,
                            path,
                            reader.valid_until,
                            reader.last_entry_id,
                            &config,
                        )?;
                        files.all.insert(entry_id, file.clone());
                        files_to_checkpoint.push(file);
                    }
                    Recovery::Abandon => {
                        let file = LogFile::write(entry_id, path, 0, None, &config)?;
                        files.all.insert(entry_id, file.clone());
                        files.inactive.push_back(file);
                    }
                }
            }
        }

        // If we recovered a file that wasn't checkpointed, activate it.
        if let Some(latest_file) = files_to_checkpoint.pop() {
            files.active = Some(latest_file);
        } else {
            files.activate_new_file(&config)?;
        }

        let (checkpoint_sender, checkpoint_receiver) = flume::unbounded();
        let wal = Self {
            data: Arc::new(Data {
                files: Mutex::new(files),
                manager: Mutex::new(Box::new(manager)),
                active_sync: Condvar::new(),
                dirfsync_sync: Condvar::new(),
                config,
                checkpoint_sender,
                checkpoint_thread: Mutex::new(None),
                readers: Mutex::default(),
                readers_sync: Condvar::new(),
            }),
        };

        for file_to_checkpoint in files_to_checkpoint {
            wal.data
                .checkpoint_sender
                .send(CheckpointCommand::Checkpoint(file_to_checkpoint))
                .to_io()?;
        }

        let weak_wal = Arc::downgrade(&wal.data);
        let mut checkpoint_thread = wal.data.checkpoint_thread.lock();
        *checkpoint_thread = Some(
            std::thread::Builder::new()
                .name(String::from("okaywal-cp"))
                .spawn(move || Self::checkpoint_thread(&weak_wal, &checkpoint_receiver))
                .expect("failed to spawn checkpointer thread"),
        );
        drop(checkpoint_thread);

        Ok(wal)
    }

    /// Begins writing an entry to this log.
    ///
    /// A new unique entry id will be allocated for the entry being written. If
    /// the entry is rolled back, the entry id will not be reused. The entry id
    /// can be retrieved through [`EntryWriter::id()`](entry::EntryWriter::id).
    ///
    /// This call will acquire an exclusive lock to the active file or block
    /// until it can be acquired.
    pub fn begin_entry(&self) -> io::Result<EntryWriter<'_, M>> {
        let mut files = self.data.files.lock();
        let file = loop {
            if let Some(file) = files.active.take() {
                break file;
            }

            // Wait for a free file
            self.data.active_sync.wait(&mut files);
        };
        files.last_entry_id.0 += 1;
        let entry_id = files.last_entry_id;
        drop(files);

        EntryWriter::new(self, entry_id, file)
    }

    fn reclaim(&self, file: LogFile<M::File>, result: WriteResult) -> io::Result<()> {
        if let WriteResult::Entry { new_length } = result {
            let last_directory_sync = if self.data.config.checkpoint_after_bytes <= new_length {
                // Checkpoint this file. This first means activating a new file.
                let mut files = self.data.files.lock();
                files.activate_new_file(&self.data.config)?;
                let last_directory_sync = files.directory_synced_at;
                drop(files);
                self.data.active_sync.notify_one();

                // Now, send the file to the checkpointer.
                self.data
                    .checkpoint_sender
                    .send(CheckpointCommand::Checkpoint(file.clone()))
                    .to_io()?;

                last_directory_sync
            } else {
                // We wrote an entry, but the file isn't ready to be
                // checkpointed. Return the file to allow more writes to happen
                // in the meantime.
                let mut files = self.data.files.lock();
                files.active = Some(file.clone());
                let last_directory_sync = files.directory_synced_at;
                drop(files);
                self.data.active_sync.notify_one();
                last_directory_sync
            };

            // Before reporting success we need to synchronize the data to
            // disk. To enable as much throughput as possible, we only want
            // one thread at a time to synchronize this file.
            file.synchronize(new_length)?;

            // If this file was activated during this process, we need to
            // sync the directory to ensure the file's metadata is
            // up-to-date.
            if let Some(created_at) = file.created_at() {
                // We want to avoid acquiring the lock again if we don't
                // need to. Verify that the file was created after the last
                // directory sync.
                if last_directory_sync.is_none() || last_directory_sync.unwrap() < created_at {
                    let files = self.data.files.lock();
                    drop(self.sync_directory(files, created_at)?);
                }
            }
        } else {
            // Nothing happened, return the file back to be written.
            let mut files = self.data.files.lock();
            files.active = Some(file);
            drop(files);
            self.data.active_sync.notify_one();
        }

        Ok(())
    }

    fn checkpoint_thread(
        data: &Weak<Data<M>>,
        checkpoint_receiver: &flume::Receiver<CheckpointCommand<M::File>>,
    ) -> io::Result<()> {
        while let Ok(CheckpointCommand::Checkpoint(file_to_checkpoint)) = checkpoint_receiver.recv()
        {
            let wal = if let Some(data) = data.upgrade() {
                WriteAheadLog { data }
            } else {
                break;
            };

            let mut writer = file_to_checkpoint.lock();
            let file_id = writer.id();
            while !writer.is_synchronized() {
                let synchronize_target = writer.position();
                writer = file_to_checkpoint.synchronize_locked(writer, synchronize_target)?;
            }
            if let Some(entry_id) = writer.last_entry_id() {
                let mut reader =
                    SegmentReader::new(writer.path(), file_id, &wal.data.config.file_manager)?;
                drop(writer);
                let mut manager = wal.data.manager.lock();
                manager.checkpoint_to(entry_id, &mut reader, &wal)?;
                writer = file_to_checkpoint.lock();
            }

            // Rename the file to denote that it's been checkpointed.
            let new_name = format!(
                "{}-cp",
                writer
                    .path()
                    .file_name()
                    .expect("missing name")
                    .to_str()
                    .expect("should be ascii")
            );
            writer.rename(&new_name)?;
            drop(writer);

            // Now, read attemps will fail, but any current readers are still
            // able to read the data. Before we truncate the file, we need to
            // wait for all existing readers to close.
            let mut readers = wal.data.readers.lock();
            while readers.get(&file_id).copied().unwrap_or(0) > 0 {
                wal.data.readers_sync.wait(&mut readers);
            }
            readers.remove(&file_id);
            drop(readers);

            // Now that there are no readers, we can safely prepare the file for
            // reuse.
            let mut writer = file_to_checkpoint.lock();
            writer.revert_to(0)?;
            drop(writer);

            let sync_target = Instant::now();

            let files = wal.data.files.lock();
            let mut files = wal.sync_directory(files, sync_target)?;
            files.inactive.push_back(file_to_checkpoint);
        }

        Ok(())
    }

    fn sync_directory<'a>(
        &'a self,
        mut files: MutexGuard<'a, Files<M::File>>,
        sync_on_or_after: Instant,
    ) -> io::Result<MutexGuard<'a, Files<M::File>>> {
        loop {
            if files.directory_synced_at.is_some()
                && files.directory_synced_at.unwrap() >= sync_on_or_after
            {
                break;
            } else if files.directory_is_syncing {
                self.data.dirfsync_sync.wait(&mut files);
            } else {
                files.directory_is_syncing = true;
                drop(files);
                let synced_at = Instant::now();
                self.data
                    .config
                    .file_manager
                    .sync_all(&self.data.config.directory)?;

                files = self.data.files.lock();
                files.directory_is_syncing = false;
                files.directory_synced_at = Some(synced_at);
                self.data.dirfsync_sync.notify_all();
                break;
            }
        }

        Ok(files)
    }

    /// Opens the log to read previously written data.
    ///
    /// # Errors
    ///
    /// May error if:
    ///
    /// - The file cannot be read.
    /// - The position refers to data that has been checkpointed.
    pub fn read_at(&self, position: LogPosition) -> io::Result<ChunkReader<'_, M>> {
        // Before opening the file to read, we need to check that this position
        // has been written to disk fully.
        let files = self.data.files.lock();
        let log_file = files
            .all
            .get(&position.file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "invalid log position"))?
            .clone();
        drop(files);

        log_file.synchronize(position.offset)?;

        let mut file = self.data.config.file_manager.open(
            &PathId::from(
                self.data
                    .config
                    .directory
                    .join(format!("wal-{}", position.file_id)),
            ),
            OpenOptions::new().read(true),
        )?;
        file.seek(SeekFrom::Start(position.offset))?;
        let mut reader = BufReader::new(file);
        let mut header_bytes = [0; 5];
        reader.read_exact(&mut header_bytes)?;
        let length = u32::from_le_bytes(header_bytes[1..5].try_into().expect("u32 is 4 bytes"));

        let mut readers = self.data.readers.lock();
        let file_readers = readers.entry(position.file_id).or_default();
        *file_readers += 1;
        drop(readers);

        Ok(ChunkReader {
            wal: self,
            file_id: position.file_id,
            reader,
            stored_crc32: None,
            length,
            bytes_remaining: length,
            read_crc32: 0,
        })
    }

    /// Waits for all other instances of [`WriteAheadLog`] to be dropped and for
    /// the checkpointing thread to complete.
    ///
    /// This call will not interrupt any writers, and will block indefinitely if
    /// another instance of this [`WriteAheadLog`] exists and is not eventually
    /// dropped. This was is the safest to implement, and because a WAL is
    /// primarily used in front of another storage layer, it follows that the
    /// shutdown logic of both layers should be synchronized.
    pub fn shutdown(self) -> io::Result<()> {
        let mut checkpoint_thread = self.data.checkpoint_thread.lock();
        let join_handle = checkpoint_thread.take().expect("shutdown already invoked");
        drop(checkpoint_thread);

        self.data
            .checkpoint_sender
            .send(CheckpointCommand::Shutdown)
            .map_err(|_| io::Error::from(ErrorKind::BrokenPipe))?;

        // Wait for the checkpoint thread to terminate.
        join_handle
            .join()
            .map_err(|_| io::Error::from(ErrorKind::BrokenPipe))?
    }
}

enum CheckpointCommand<F>
where
    F: file_manager::File,
{
    Checkpoint(LogFile<F>),
    Shutdown,
}

#[derive(Debug)]
struct Files<F>
where
    F: file_manager::File,
{
    active: Option<LogFile<F>>,
    inactive: VecDeque<LogFile<F>>,
    last_entry_id: EntryId,
    directory_synced_at: Option<Instant>,
    directory_is_syncing: bool,
    all: HashMap<u64, LogFile<F>>,
}

impl<F> Files<F>
where
    F: file_manager::File,
{
    pub fn activate_new_file(&mut self, config: &Configuration<F::Manager>) -> io::Result<()> {
        let next_id = self.last_entry_id.0 + 1;
        let file_name = format!("wal-{next_id}");
        let file = if let Some(file) = self.inactive.pop_front() {
            let old_id = file.lock().id();
            file.rename(next_id, &file_name)?;
            let all_entry = self.all.remove(&old_id).expect("missing all entry");
            self.all.insert(next_id, all_entry);
            file
        } else {
            let file = LogFile::write(
                next_id,
                config.directory.join(file_name).into(),
                0,
                None,
                config,
            )?;
            self.all.insert(next_id, file.clone());
            file
        };

        self.active = Some(file);

        Ok(())
    }
}

impl<F> Default for Files<F>
where
    F: file_manager::File,
{
    fn default() -> Self {
        Self {
            active: None,
            inactive: VecDeque::new(),
            last_entry_id: EntryId::default(),
            directory_synced_at: None,
            directory_is_syncing: false,
            all: HashMap::new(),
        }
    }
}

#[derive(Clone, Copy)]
enum WriteResult {
    RolledBack,
    Entry { new_length: u64 },
}

/// A buffered reader for a previously written data chunk.
///
/// This reader will stop returning bytes after reading all bytes previously
/// written.
#[derive(Debug)]
pub struct ChunkReader<'a, M>
where
    M: FileManager,
{
    wal: &'a WriteAheadLog<M>,
    file_id: u64,
    reader: BufReader<M::File>,
    bytes_remaining: u32,
    length: u32,
    stored_crc32: Option<u32>,
    read_crc32: u32,
}

impl<'a, M> ChunkReader<'a, M>
where
    M: FileManager,
{
    /// Returns the length of the data stored.
    ///
    /// This value will not change as data is read.
    #[must_use]
    pub const fn chunk_length(&self) -> u32 {
        self.length
    }

    /// Returns the number of bytes remaining to read.
    ///
    /// This value will be updated as read calls return data.
    #[must_use]
    pub const fn bytes_remaining(&self) -> u32 {
        self.bytes_remaining
    }

    /// Returns true if the stored checksum matches the computed checksum during
    /// read.
    ///
    /// This function will only return `Ok()` if the chunk has been fully read.
    pub fn crc_is_valid(&mut self) -> io::Result<bool> {
        if self.bytes_remaining == 0 {
            if self.stored_crc32.is_none() {
                let mut stored_crc32 = [0; 4];
                // Bypass our internal read, otherwise our crc would include the
                // crc read itself.
                self.reader.read_exact(&mut stored_crc32)?;
                self.stored_crc32 = Some(u32::from_le_bytes(stored_crc32));
            }

            Ok(self.stored_crc32.expect("already initialized") == self.read_crc32)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "crc cannot be checked before reading all chunk bytes",
            ))
        }
    }
}

impl<'a, M> Read for ChunkReader<'a, M>
where
    M: FileManager,
{
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let bytes_remaining =
            usize::try_from(self.bytes_remaining).expect("too large for platform");
        if buf.len() > bytes_remaining {
            buf = &mut buf[..bytes_remaining];
        }
        let bytes_read = self.reader.read(buf)?;
        self.read_crc32 = crc32c::crc32c_append(self.read_crc32, &buf[..bytes_read]);
        self.bytes_remaining -= u32::try_from(bytes_read).expect("can't be larger than buf.len()");
        Ok(bytes_read)
    }
}

impl<'a, M> Drop for ChunkReader<'a, M>
where
    M: FileManager,
{
    fn drop(&mut self) {
        let mut readers = self.wal.data.readers.lock();
        let file_readers = readers
            .get_mut(&self.file_id)
            .expect("reader entry not present");
        *file_readers -= 1;
        drop(readers);
        self.wal.data.readers_sync.notify_one();
    }
}

#[cfg(test)]
mod tests;
