use std::{
    io,
    ops::Mul,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{LogManager, WriteAheadLog};

#[derive(Debug)]
pub struct Configuration {
    pub directory: PathBuf,
    pub preallocate_bytes: u32,
    pub checkpoint_after_bytes: u64,
    pub buffer_bytes: usize,
    pub version_info: Arc<Vec<u8>>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self::default_for("okaywal")
    }
}

impl Configuration {
    pub fn default_for<P: AsRef<Path>>(path: P) -> Self {
        Self {
            directory: path.as_ref().to_path_buf(),
            preallocate_bytes: megabytes(1),
            checkpoint_after_bytes: megabytes(1),
            buffer_bytes: kilobytes(16),
            version_info: Arc::default(),
        }
    }

    pub fn open<Manager: LogManager>(self, manager: Manager) -> io::Result<WriteAheadLog> {
        WriteAheadLog::open(self, manager)
    }
}

fn megabytes<T: Mul<Output = T> + From<u16>>(megs: T) -> T {
    kilobytes(megs) * T::from(1024)
}

fn kilobytes<T: Mul<Output = T> + From<u16>>(bytes: T) -> T {
    bytes * T::from(1024)
}
