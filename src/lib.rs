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

use file::AnyFile;
use parking_lot::{Condvar, Mutex};
use watchable::{Watchable, Watcher};

use crate::file::{AnyFileKind, AnyFileManager};

#[derive(Debug, Clone)]
pub struct WriteAheadLog {
    data: Arc<Data>,
}

impl WriteAheadLog {
    pub fn recover(
        directory: impl AsRef<Path>,
        archiver: impl Archiver + 'static,
    ) -> io::Result<Self> {
        Self::recover_with_config(directory, archiver, Config::default())
    }

    pub fn recover_with_config(
        directory: impl AsRef<Path>,
        mut archiver: impl Archiver + 'static,
        config: Config,
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
                        writer: Some(AnyFile {
                            length: Some(length),
                            position: 0,
                            file: AnyFileKind::Std { file },
                        }),
                        active: false,
                    });
                } else {
                    break;
                }
            }
        } else {
            config.file_manager.create_dir_recursive(&directory)?;
        }

        let latest_entry_id = Self::recover_files(&mut segments, &mut archiver, &config)?;

        segments.segment_sync_index =
            u16::try_from(segments.slots.len().saturating_sub(1)).to_io()?;

        let watchable = Watchable::new(EntryId(0));
        let watcher = watchable.watch();
        let wal = Self {
            data: Arc::new(Data {
                next_entry_id: AtomicU64::new(latest_entry_id.0 + 1),
                directory,
                config,
                archiver: Mutex::new(Box::new(archiver)),
                segments: Mutex::new(segments),
                segments_sync: Condvar::new(),
                dirsync_sync: Condvar::new(),
                archive_sync: Condvar::new(),
                archive_to_sender: watchable,
            }),
        };
        let thread_wal = Arc::downgrade(&wal.data);
        std::thread::Builder::new()
            .name(String::from("okaywal-cp"))
            .spawn(move || Self::archive_loop(thread_wal, watcher))
            .expect("thread spawn failed");

        Ok(wal)
    }

    pub fn write(&self) -> io::Result<LogWriter> {
        let mut segments = self.data.segments.lock();

        loop {
            if let Some(slot_id) = segments.active_slots.pop_front() {
                let file = segments.slots[usize::from(slot_id)]
                    .writer
                    .take()
                    .expect("missing file");

                return LogWriter::new(self.clone(), slot_id, file);
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

                    return LogWriter::new(self.clone(), u16::try_from(slot_id).to_io()?, file);
                }

                // Open a new segment file
                segments.active_slot_count += 1;

                let slot_id = segments.slots.len();
                let path = self.data.directory.join(format!("wal{slot_id}"));
                let mut file = self.data.config.file_manager.append_to_path(&path)?;

                self.data.config.write_segment_header(&mut file)?;

                segments.slots.push(FileSlot {
                    path: Arc::new(path),
                    first_entry_id: None,
                    latest_entry_id: None,
                    writer: None,
                    active: true,
                });

                return LogWriter::new(self.clone(), u16::try_from(slot_id).to_io()?, file);
            } else {
                self.data.segments_sync.wait(&mut segments);
            }
        }
    }

    pub fn read_at(&self, position: LogPosition) -> io::Result<AnyFile> {
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
        file: AnyFile,
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

            segments.unarchived_bytes += bytes_written;
            if segments.unarchived_bytes >= self.data.config.archive_after_bytes {
                segments.unarchived_bytes = 0;
                self.data.archive_to_sender.replace(entry_id);
                segments.remove_segments_for_archiving(entry_id);
            }
        }

        let archive_to = *self.data.archive_to_sender.write();
        let segment_made_available = if segments.slots[slot_index].first_entry_id.is_some()
            && segments.slots[slot_index].first_entry_id.unwrap() <= archive_to
        {
            segments.slots[slot_index].active = false;
            segments.active_slot_count -= 1;
            let latest_id = segments.slots[slot_index]
                .latest_entry_id
                .expect("always present if first_entry_id is");
            if archive_to < latest_id {
                self.data.archive_to_sender.replace(latest_id);

                segments.remove_segments_for_archiving(archive_to);
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
            self.data.archive_sync.notify_one();
        }

        Ok(())
    }

    fn recover_files<A: Archiver>(
        files: &mut SegmentFiles,
        archiver: &mut A,
        config: &Config,
    ) -> io::Result<EntryId> {
        let mut readers = Vec::with_capacity(files.slots.len());
        let mut completed_readers = Vec::with_capacity(files.slots.len());
        let mut latest_entry_id = 0;
        for (index, slot) in files.slots.iter().enumerate() {
            let file = config.file_manager.read_path(&slot.path)?;
            let reader = SegmentReader::new(index, file)?;
            match archiver.should_recover_segment(&reader.header)? {
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

                archiver.recover(&mut Entry {
                    id: reader.current_entry_id.unwrap(),
                    reader,
                })?;
                latest_entry_id = reader.current_entry_id.unwrap().0;

                if !reader.read_next_entry()? {
                    // No more entries.
                    readers.remove(lowest_entry_reader);
                }
            }
        }

        // We've now informed the archiver of all commits, we need to clean up
        // the files (truncating any invalid data) and enqueue them.
        for reader in completed_readers {
            let slot = &mut files.slots[reader.slot_index];
            slot.first_entry_id = reader.first_entry_id;
            slot.latest_entry_id = reader.last_entry_id;
            let writer = slot.writer.as_mut().unwrap();
            if slot.first_entry_id.is_none() {
                writer.set_len(0)?;
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

    fn archive_loop(data: Weak<Data>, mut archive_to: Watcher<EntryId>) {
        for mut entry_to_archive_to in &mut archive_to {
            let data = if let Some(data) = data.upgrade() {
                data
            } else {
                // The final instance was dropped before we could archive.
                break;
            };

            let mut segments = data.segments.lock();
            // Wait for all outstanding files that have an EntryId that is <=
            // entry_to_archive_to
            while !segments
                .slots
                .iter()
                .all(|slot| slot.can_archive_if_needed(entry_to_archive_to))
            {
                data.archive_sync.wait(&mut segments);
                // Other files may have been returned that need to be
                // archived but have a later entry id.
                entry_to_archive_to = *data.archive_to_sender.write();
            }

            let mut archiver = data.archiver.lock();
            // TODO error handling?
            archiver.archive(entry_to_archive_to).unwrap();

            for slot in segments
                .slots
                .iter_mut()
                .filter(|slot| slot.should_archive(entry_to_archive_to))
            {
                let writer = slot.writer.as_mut().unwrap();
                // TODO error handling?
                writer.set_len(0).unwrap();
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
    archiver: Mutex<Box<dyn Archiver>>,
    segments: Mutex<SegmentFiles>,
    segments_sync: Condvar,
    dirsync_sync: Condvar,
    archive_sync: Condvar,
    archive_to_sender: Watchable<EntryId>,
    config: Config,
}

#[derive(Debug)]
pub struct Config {
    pub version_info: Vec<u8>,
    pub active_segment_limit: usize,
    pub archive_after_bytes: u64,
    pub file_manager: AnyFileManager,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version_info: Vec::default(),
            active_segment_limit: 8,
            archive_after_bytes: 8 * 1024 * 1024,
            file_manager: AnyFileManager::Std,
        }
    }
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if self.version_info.len() > 255 {
            return Err(io::Error::new(
                ErrorKind::Other,
                "version_info must be <= 255 bytes",
            ));
        }

        Ok(())
    }

    fn write_segment_header(&self, file: &mut AnyFile) -> io::Result<()> {
        let mut buf = BufWriter::new(file);
        buf.write_all(b"okw\0")?;
        let version_size = u8::try_from(self.version_info.len()).to_io()?;
        buf.write_all(&[version_size])?;
        buf.write_all(&self.version_info)?;
        buf.flush()?;

        Ok(())
    }
}

pub trait Archiver: Send + Sync + Debug {
    fn should_recover_segment(&mut self, segment: &RecoveredSegment) -> io::Result<Recovery>;
    fn recover(&mut self, entry: &mut Entry<'_>) -> io::Result<()>;

    fn archive(&mut self, last_archived_id: EntryId) -> io::Result<()>;
}

pub struct RecoveredSegment {
    pub version_info: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default)]
pub struct EntryId(pub u64);

pub struct Entry<'a> {
    pub id: EntryId,
    reader: &'a mut SegmentReader,
}

impl<'entry> Entry<'entry> {
    pub fn read_chunk<'chunk>(&'chunk mut self) -> io::Result<Option<EntryData<'chunk, 'entry>>> {
        if self.reader.file.buffer().len() < 9 {
            self.reader.file.fill_buf()?;
        }

        if self.reader.file.buffer().len() > 9 && self.reader.file.buffer()[0] == CHUNK {
            let mut header_bytes = [0; 9];
            self.reader.file.read_exact(&mut header_bytes)?;
            return Ok(Some(EntryData {
                entry: self,
                calculated_crc: 0,
                stored_crc: u32::from_le_bytes(
                    header_bytes[1..5].try_into().expect("u32 is 4 bytes"),
                ),
                bytes_remaining: u32::from_le_bytes(
                    header_bytes[5..].try_into().expect("u32 is 4 bytes"),
                ),
            }));
        }

        Ok(None)
    }
}

pub struct EntryData<'chunk, 'entry> {
    entry: &'chunk mut Entry<'entry>,
    bytes_remaining: u32,
    stored_crc: u32,
    calculated_crc: u32,
}

impl<'chunk, 'entry> EntryData<'chunk, 'entry> {
    pub const fn check_crc(&self) -> Option<bool> {
        if self.bytes_remaining > 0 {
            None
        } else {
            Some(self.stored_crc == self.calculated_crc)
        }
    }
}

impl<'chunk, 'entry> Read for EntryData<'chunk, 'entry> {
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

pub enum Recovery {
    Recover,
    Abandon,
}

#[derive(Debug, Default)]
struct SegmentFiles {
    directory_is_syncing: bool,
    segment_sync_index: u16,
    slots: Vec<FileSlot>,
    active_slots: VecDeque<u16>,
    active_slot_count: usize,
    unarchived_bytes: u64,
}

impl SegmentFiles {
    fn remove_segments_for_archiving(&mut self, archive_to: EntryId) {
        let mut active_slot_index = 0;
        while active_slot_index < self.active_slots.len() {
            let slot = &mut self.slots[usize::from(self.active_slots[active_slot_index])];
            if slot.first_entry_id.is_some() && slot.first_entry_id.unwrap() <= archive_to {
                slot.active = false;
                // This segment contains data that needs to be archived.
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
    writer: Option<AnyFile>,
    active: bool,
}

impl FileSlot {
    fn should_archive(&self, archive_to: EntryId) -> bool {
        self.first_entry_id.is_some() && self.first_entry_id.unwrap() <= archive_to
    }

    fn can_archive_if_needed(&self, archive_to: EntryId) -> bool {
        let should_archive = self.should_archive(archive_to);
        !should_archive || self.writer.is_some()
    }
}

pub struct CompleteSegments {}

pub struct LogWriter {
    pub entry_id: EntryId,
    log: WriteAheadLog,
    slot_id: u16,
    file: Option<BufWriter<AnyFile>>,
    original_file_length: u64,
    bytes_written: u64,
}

const NEW_ENTRY: u8 = 1;
const CHUNK: u8 = 2;

impl LogWriter {
    fn new(log: WriteAheadLog, slot_id: u16, file: AnyFile) -> io::Result<Self> {
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

    pub fn commit(mut self) -> io::Result<EntryId> {
        let mut file = self.file.take().expect("already committed").into_inner()?;
        file.sync_data()?;

        self.log.reclaim(
            self.slot_id,
            file,
            Some(self.entry_id),
            Some(self.bytes_written),
        )?;

        Ok(self.entry_id)
    }

    pub fn write_all(&mut self, data: &[u8]) -> io::Result<DataRecord> {
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

        Ok(DataRecord {
            position,
            crc,
            length,
        })
    }

    pub fn position(&self) -> io::Result<LogPosition> {
        let file = self.file.as_ref().expect("already dropped");
        let offset = file.get_ref().position();
        let pending_bytes = u64::try_from(file.buffer().len()).to_io()?;
        Ok(LogPosition {
            slot_id: self.slot_id,
            offset: offset + pending_bytes,
        })
    }

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

impl Drop for LogWriter {
    fn drop(&mut self) {
        if self.file.is_some() {
            self.rollback_session().unwrap()
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct LogPosition {
    slot_id: u16,
    offset: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DataRecord {
    pub position: LogPosition,
    pub crc: u32,
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

struct SegmentReader {
    slot_index: usize,
    file: BufReader<AnyFile>,
    header: RecoveredSegment,
    current_entry_id: Option<EntryId>,
    first_entry_id: Option<EntryId>,
    last_entry_id: Option<EntryId>,
    valid_until: u64,
}

impl SegmentReader {
    pub fn new(slot_id: usize, file: AnyFile) -> io::Result<Self> {
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

        let mut reader = Self {
            slot_index: slot_id,
            file,
            header,
            current_entry_id: None,
            first_entry_id: None,
            last_entry_id: None,
            valid_until: u64::from(version_info_length) + 5,
        };

        reader.read_next_entry()?;

        Ok(reader)
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

#[derive(Debug)]
pub struct VoidArchiver;

impl Archiver for VoidArchiver {
    fn should_recover_segment(&mut self, _segment: &RecoveredSegment) -> io::Result<Recovery> {
        Ok(Recovery::Abandon)
    }

    fn recover(&mut self, _entry: &mut Entry<'_>) -> io::Result<()> {
        Ok(())
    }

    fn archive(&mut self, _last_archived_id: EntryId) -> io::Result<()> {
        Ok(())
    }
}
