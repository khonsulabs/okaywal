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
    collections::VecDeque,
    fs::{self, File},
    io::{self, ErrorKind, Seek, SeekFrom},
    path::Path,
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::Instant,
};

use parking_lot::{Condvar, Mutex, MutexGuard};

pub use crate::{
    config::Configuration,
    entry::{EntryId, EntryWriter},
    log_file::{Entry, EntryChunk, ReadChunkResult, RecoveredSegment, SegmentReader},
    manager::{LogManager, LogVoid, Recovery},
};
use crate::{entry::LogPosition, log_file::LogFile, to_io_result::ToIoResult};

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
pub struct WriteAheadLog {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    files: Mutex<Files>,
    manager: Mutex<Box<dyn LogManager>>,
    active_sync: Condvar,
    dirfsync_sync: Condvar,
    config: Configuration,
    checkpoint_sender: flume::Sender<LogFile>,
    checkpoint_thread: Mutex<Option<JoinHandle<io::Result<()>>>>,
}

#[derive(Debug, Default)]
struct Files {
    active: Option<LogFile>,
    inactive: VecDeque<LogFile>,
    last_entry_id: EntryId,
    directory_synced_at: Option<Instant>,
    directory_is_syncing: bool,
}

impl WriteAheadLog {
    /// Creates or opens a log in `directory` using the provided log manager to
    /// drive recovery and checkpointing.
    pub fn recover<AsRefPath: AsRef<Path>, Manager: LogManager>(
        directory: AsRefPath,
        manager: Manager,
    ) -> io::Result<Self> {
        Configuration::default_for(directory).open(manager)
    }

    fn open<Manager: LogManager>(config: Configuration, mut manager: Manager) -> io::Result<Self> {
        if !config.directory.exists() {
            std::fs::create_dir_all(&config.directory)?;
        }
        // Find all files that match this pattern: "wal-[0-9]+(-cp)?"
        let mut discovered_files = Vec::new();
        for file in fs::read_dir(&config.directory)?.filter_map(Result::ok) {
            if let Some(file_name) = file.file_name().to_str() {
                let mut parts = file_name.split('-');
                let prefix = parts.next();
                let entry_id = parts.next().and_then(|ts| ts.parse::<u64>().ok());
                let suffix = parts.next();
                match (prefix, entry_id, suffix) {
                    (Some(prefix), Some(entry_id), suffix) if prefix == "wal" => {
                        let has_checkpointed = suffix == Some("cp");
                        discovered_files.push((entry_id, file.path(), has_checkpointed));
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
            if has_checkpointed {
                files
                    .inactive
                    .push_back(LogFile::write(entry_id, path, 0, None, &config)?);
            } else {
                let mut reader = SegmentReader::new(&path)?;
                match manager.should_recover_segment(&reader.header)? {
                    Recovery::Recover => {
                        while let Some(mut entry) = reader.read_entry()? {
                            manager.recover(&mut entry)?;
                            while let Some(mut chunk) = match entry.read_chunk()? {
                                ReadChunkResult::Chunk(chunk) => Some(chunk),
                                ReadChunkResult::EndOfEntry | ReadChunkResult::AbortedEntry => None,
                            } {
                                chunk.skip_remaining_bytes()?;
                            }
                            files.last_entry_id = entry.id();
                        }

                        files_to_checkpoint.push(LogFile::write(
                            entry_id,
                            path,
                            reader.valid_until,
                            reader.last_entry_id,
                            &config,
                        )?);
                    }
                    Recovery::Abandon => files
                        .inactive
                        .push_back(LogFile::write(entry_id, path, 0, None, &config)?),
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
            }),
        };

        for file_to_checkpoint in files_to_checkpoint.into_iter().rev() {
            wal.data
                .checkpoint_sender
                .send(file_to_checkpoint)
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
    pub fn begin_entry(&self) -> io::Result<EntryWriter<'_>> {
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

    fn reclaim(&self, file: LogFile, result: WriteResult) -> io::Result<()> {
        if let WriteResult::Entry { new_length } = result {
            let last_directory_sync = if self.data.config.checkpoint_after_bytes <= new_length {
                // Checkpoint this file. This first means activating a new file.
                let mut files = self.data.files.lock();
                files.activate_new_file(&self.data.config)?;
                let last_directory_sync = files.directory_synced_at;
                drop(files);
                self.data.active_sync.notify_one();

                // Now, send the file to the checkpointer.
                self.data.checkpoint_sender.send(file.clone()).to_io()?;

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
        data: &Weak<Data>,
        checkpoint_receiver: &flume::Receiver<LogFile>,
    ) -> io::Result<()> {
        while let Ok(file_to_checkpoint) = checkpoint_receiver.recv() {
            let wal = if let Some(data) = data.upgrade() {
                WriteAheadLog { data }
            } else {
                break;
            };

            let mut writer = file_to_checkpoint.lock();
            while !writer.is_synchronized() {
                let synchronize_target = writer.position();
                writer = file_to_checkpoint.synchronize_locked(writer, synchronize_target)?;
            }
            if let Some(entry_id) = writer.last_entry_id() {
                let mut reader = SegmentReader::new(writer.path())?;
                let mut manager = wal.data.manager.lock();
                manager.checkpoint_to(entry_id, &mut reader)?;
            }

            // Prepare the writer to be recycled.
            let new_name = format!(
                "{}-cp",
                writer
                    .path()
                    .file_name()
                    .expect("missing name")
                    .to_str()
                    .expect("should be ascii")
            );
            writer.revert_to(0)?;
            drop(writer);

            file_to_checkpoint.rename(&new_name)?;
            let sync_target = Instant::now();

            let files = wal.data.files.lock();
            let mut files = wal.sync_directory(files, sync_target)?;
            files.inactive.push_back(file_to_checkpoint);
        }

        Ok(())
    }

    fn sync_directory<'a>(
        &'a self,
        mut files: MutexGuard<'a, Files>,
        sync_on_or_after: Instant,
    ) -> io::Result<MutexGuard<'a, Files>> {
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
                let directory = File::open(&self.data.config.directory)?;
                let synced_at = Instant::now();
                directory.sync_all()?;

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
    pub fn read_at(&self, position: LogPosition) -> io::Result<File> {
        let mut file = File::open(
            self.data
                .config
                .directory
                .join(format!("wal-{}", position.file_id)),
        )?;
        file.seek(SeekFrom::Start(position.offset + 9))?;
        Ok(file)
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
        // Dropping self allows the flume::Sender to be freed, as the checkpoint
        // thread only has a weak reference while receiving the next file to
        // checkpoint.
        drop(self);

        // Wait for the checkpoint thread to terminate.
        join_handle
            .join()
            .map_err(|_| io::Error::from(ErrorKind::BrokenPipe))?
    }
}

impl Files {
    pub fn activate_new_file(&mut self, config: &Configuration) -> io::Result<()> {
        let next_id = self.last_entry_id.0 + 1;
        let file_name = format!("wal-{next_id}");
        let file = if let Some(file) = self.inactive.pop_front() {
            file.rename(&file_name)?;
            file
        } else {
            LogFile::write(next_id, config.directory.join(file_name), 0, None, config)?
        };

        self.active = Some(file);

        Ok(())
    }
}

#[derive(Clone, Copy)]
enum WriteResult {
    RolledBack,
    Entry { new_length: u64 },
}

#[cfg(test)]
mod tests;
