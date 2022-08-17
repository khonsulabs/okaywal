use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::{Mutex, RwLock};

use crate::ToIoResult;

#[derive(Debug, Clone)]
pub enum AnyFileManager {
    Std,
    Memory(MemoryFiles),
}

impl AnyFileManager {
    pub fn path_exists(&self, path: &Path) -> bool {
        match self {
            AnyFileManager::Std => path.exists(),
            AnyFileManager::Memory(files) => {
                let files = files.0.lock();
                files.contains_key(path)
            }
        }
    }

    pub fn append_to_path(&self, path: &Path) -> io::Result<AnyFile> {
        match self {
            AnyFileManager::Std => {
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&path)?;
                let length = file.metadata()?.len();
                Ok(AnyFile {
                    length: Some(length),
                    position: 0,
                    file: AnyFileKind::Std { file },
                })
            }
            AnyFileManager::Memory(files) => {
                let mut files = files.0.lock();

                let file = files.entry(path.to_path_buf()).or_default().clone();
                let data = file.0.read();
                let length = u64::try_from(data.len()).to_io()?;
                drop(data);

                Ok(AnyFile {
                    length: Some(length),
                    position: 0,
                    file: AnyFileKind::Memory(file),
                })
            }
        }
    }

    pub fn read_path(&self, path: &Path) -> io::Result<AnyFile> {
        match self {
            AnyFileManager::Std => {
                let file = OpenOptions::new().read(true).open(&path)?;
                Ok(AnyFile {
                    length: None,
                    position: 0,
                    file: AnyFileKind::Std { file },
                })
            }
            AnyFileManager::Memory(files) => {
                let files = files.0.lock();
                if let Some(file) = files.get(path) {
                    Ok(AnyFile {
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
            AnyFileManager::Std => {
                let file = OpenOptions::new().read(true).open(path)?;
                file.sync_all()
            }
            AnyFileManager::Memory(_) => Ok(()),
        }
    }

    pub fn create_dir_recursive(&self, path: &Path) -> io::Result<()> {
        if let AnyFileManager::Std = self {
            return std::fs::create_dir_all(path);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct AnyFile {
    pub length: Option<u64>,
    pub position: u64,
    pub file: AnyFileKind,
}

#[derive(Debug)]
pub enum AnyFileKind {
    Std { file: File },
    Memory(MemoryFile),
}

impl AnyFile {
    pub fn memory() -> Self {
        Self {
            length: None,
            position: 0,
            file: AnyFileKind::Memory(MemoryFile::default()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryFile(Arc<RwLock<Vec<u8>>>);

impl AnyFile {
    pub fn len(&self) -> io::Result<u64> {
        if let Some(length) = self.length {
            Ok(length)
        } else {
            match &self.file {
                AnyFileKind::Std { file } => Ok(file.metadata()?.len()),
                AnyFileKind::Memory(_) => todo!(),
            }
        }
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn set_len(&mut self, new_length: u64) -> io::Result<()> {
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

    pub fn sync_data(&mut self) -> io::Result<()> {
        match &mut self.file {
            AnyFileKind::Std { file } => file.sync_data(),
            AnyFileKind::Memory(_) => Ok(()),
        }
    }
}

impl Write for AnyFile {
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

impl Seek for AnyFile {
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

impl Read for AnyFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = match &mut self.file {
            AnyFileKind::Std { file } => file.read(buf)?,
            AnyFileKind::Memory(_) => todo!(),
        };

        self.position += u64::try_from(bytes_read).to_io()?;

        Ok(bytes_read)
    }
}
