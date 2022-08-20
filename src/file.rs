use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{self, ErrorKind, Read, Seek, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Arc,
};

use fs2::FileExt;
use parking_lot::{Mutex, RwLock};

use crate::ToIoResult;

#[derive(Debug, Clone)]
pub enum FileManager {
    Std,
    Memory(MemoryFiles),
}

impl FileManager {
    pub fn path_exists(&self, path: &Path) -> bool {
        match self {
            FileManager::Std => path.exists(),
            FileManager::Memory(files) => {
                let files = files.0.lock();
                files.contains_key(path)
            }
        }
    }

    pub fn append_to_path(&self, path: &Path) -> io::Result<File> {
        match self {
            FileManager::Std => {
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&path)?;
                let length = file.metadata()?.len();
                Ok(File {
                    length: Some(length),
                    position: 0,
                    file: AnyFileKind::Std { file },
                })
            }
            FileManager::Memory(files) => {
                let mut files = files.0.lock();

                let file = files.entry(path.to_path_buf()).or_default().clone();
                let data = file.0.read();
                let length = u64::try_from(data.len()).to_io()?;
                drop(data);

                Ok(File {
                    length: Some(length),
                    position: 0,
                    file: AnyFileKind::Memory(file),
                })
            }
        }
    }

    pub fn read_path(&self, path: &Path) -> io::Result<File> {
        match self {
            FileManager::Std => {
                let file = OpenOptions::new().read(true).open(&path)?;
                Ok(File {
                    length: None,
                    position: 0,
                    file: AnyFileKind::Std { file },
                })
            }
            FileManager::Memory(files) => {
                let files = files.0.lock();
                if let Some(file) = files.get(path) {
                    Ok(File {
                        length: None,
                        position: 0,
                        file: AnyFileKind::Memory(file.clone()),
                    })
                } else {
                    Err(io::Error::from(ErrorKind::NotFound))
                }
            }
        }
    }

    pub fn synchronize_path(&self, path: &Path) -> io::Result<()> {
        match self {
            FileManager::Std => {
                let file = OpenOptions::new().read(true).open(path)?;
                file.sync_all()
            }
            FileManager::Memory(_) => Ok(()),
        }
    }

    pub fn create_dir_recursive(&self, path: &Path) -> io::Result<()> {
        if let FileManager::Std = self {
            return std::fs::create_dir_all(path);
        }
        Ok(())
    }
}

/// An open file.
#[derive(Debug)]
pub struct File {
    pub(crate) length: Option<u64>,
    pub(crate) position: u64,
    pub(crate) file: AnyFileKind,
}

#[derive(Debug)]
pub enum AnyFileKind {
    Std { file: fs::File },
    Memory(MemoryFile),
}

impl File {
    pub(crate) fn memory() -> Self {
        Self {
            length: None,
            position: 0,
            file: AnyFileKind::Memory(MemoryFile::default()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryFile(Arc<RwLock<Vec<u8>>>);

impl File {
    pub(crate) fn len(&self) -> io::Result<u64> {
        if let Some(length) = self.length {
            Ok(length)
        } else {
            match &self.file {
                AnyFileKind::Std { file } => Ok(file.metadata()?.len()),
                AnyFileKind::Memory(_) => todo!(),
            }
        }
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    pub(crate) fn set_len(&mut self, new_length: u64) -> io::Result<()> {
        let length = self.length.as_mut().expect("writing to a reader");
        let was_at_end = *length == self.position;
        match &mut self.file {
            AnyFileKind::Std { file } => {
                file.set_len(new_length)?;
            }
            AnyFileKind::Memory(_) => todo!(),
        }
        *length = new_length;
        if was_at_end || self.position > new_length {
            self.position = new_length;
        }

        Ok(())
    }

    pub(crate) fn sync_data(&mut self) -> io::Result<()> {
        match &mut self.file {
            AnyFileKind::Std { file } => file.sync_data(),
            AnyFileKind::Memory(_) => Ok(()),
        }
    }

    pub(crate) fn lock_exclusive(&self) -> io::Result<()> {
        match &self.file {
            AnyFileKind::Std { file } => file.lock_exclusive(),
            AnyFileKind::Memory(_) => Ok(()),
        }
    }

    pub(crate) fn unlock(&self) -> io::Result<()> {
        match &self.file {
            AnyFileKind::Std { file } => file.unlock(),
            AnyFileKind::Memory(_) => Ok(()),
        }
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = match &mut self.file {
            AnyFileKind::Std { file } => match file.write(buf) {
                Ok(bytes_written) => bytes_written,
                err => return err,
            },
            AnyFileKind::Memory(_) => todo!(),
        };

        let length = self.length.as_mut().expect("writing to a reader");
        let bytes_remaining = *length - self.position;
        let bytes_written_as_u64 = u64::try_from(bytes_written).to_io()?;
        self.position += bytes_written_as_u64;
        if bytes_remaining < bytes_written_as_u64 {
            // This write extended the file's length
            let overwrite_by = bytes_written_as_u64 - bytes_remaining;
            *length += overwrite_by;
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.file {
            AnyFileKind::Std { file } => file.flush(),
            AnyFileKind::Memory(_) => todo!(),
        }
    }
}

impl Seek for File {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.position = match &mut self.file {
            AnyFileKind::Std { file } => file.seek(pos)?,
            AnyFileKind::Memory(_) => todo!(),
        };

        Ok(self.position)
    }
}

#[derive(Clone, Debug)]
pub struct MemoryFiles(Arc<Mutex<HashMap<PathBuf, MemoryFile>>>);

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = match &mut self.file {
            AnyFileKind::Std { file } => file.read(buf)?,
            AnyFileKind::Memory(_) => todo!(),
        };

        self.position += u64::try_from(bytes_read).to_io()?;

        Ok(bytes_read)
    }
}

/// An open file that has an exclusive file lock.
#[derive(Debug)]
pub struct LockedFile {
    file: File,
    locked: bool,
}

impl LockedFile {
    pub(crate) fn new(file: File) -> Self {
        let locked = file.lock_exclusive().is_ok();
        Self { file, locked }
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        if self.locked {
            drop(self.file.unlock());
        }
    }
}

impl Deref for LockedFile {
    type Target = File;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for LockedFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Write for LockedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for LockedFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Read for LockedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}
