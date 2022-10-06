use crate::to_io_result::ToIoResult;
use std::{
    fs,
    io::{self, Seek, SeekFrom, Write},
};

#[derive(Debug)]
pub struct Buffered<F>
where
    F: Bufferable + Seek + Write,
{
    buffer: Vec<u8>,
    position: u64,
    buffer_write_position: usize,
    length: u64,
    file: F,
}

impl<F> Buffered<F>
where
    F: Bufferable + Seek + Write,
{
    pub fn with_capacity(mut file: F, capacity: usize) -> io::Result<Self> {
        let length = file.len()?;
        let position = file.stream_position()?;
        Ok(Self {
            buffer: Vec::with_capacity(capacity),
            position,
            buffer_write_position: 0,
            length,
            file,
        })
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            self.file.write_all(&self.buffer)?;
            let bytes_written = u64::try_from(self.buffer.len()).to_io()?;
            self.position += bytes_written;
            self.length = self.length.max(self.position);
            self.buffer_write_position = 0;
            self.buffer.clear();
        }
        Ok(())
    }

    pub fn position(&self) -> u64 {
        self.position + u64::try_from(self.buffer_write_position).expect("impossibly large buffer")
    }

    pub const fn buffer_position(&self) -> u64 {
        self.position
    }

    pub fn inner(&self) -> &F {
        &self.file
    }
}

impl<F> Write for Buffered<F>
where
    F: Bufferable + Seek + Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buffer.capacity() == self.buffer_write_position {
            self.flush_buffer()?;
        }

        // If what we're writing is larger than our buffer, skip the buffer
        // entirely.
        if buf.len() > self.buffer.capacity() {
            // Ensure what we've buffered is already written.
            self.flush_buffer()?;
            let bytes_written = self.file.write(buf)?;
            self.position += u64::try_from(bytes_written).to_io()?;
            return Ok(bytes_written);
        }

        let bytes_remaining = self.buffer.capacity() - self.buffer_write_position;
        let bytes_to_write = buf.len().min(bytes_remaining);
        if bytes_to_write > 0 {
            let bytes_to_copy =
                (self.buffer.len() - self.buffer_write_position).min(bytes_to_write);
            if bytes_to_copy > 0 {
                self.buffer[self.buffer_write_position..].copy_from_slice(&buf[..bytes_to_copy]);
            }
            let bytes_to_extend = bytes_to_write - bytes_to_copy;
            if bytes_to_extend > 0 {
                self.buffer
                    .extend_from_slice(&buf[bytes_to_copy..bytes_to_write]);
            }
            self.buffer_write_position += bytes_to_write;
        }

        Ok(bytes_to_write)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.file.flush()?;
        Ok(())
    }
}

impl<F> Seek for Buffered<F>
where
    F: Bufferable + Seek + Write,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_position = match pos {
            SeekFrom::Start(position) => position,
            SeekFrom::End(offset) => {
                if let Ok(offset) = u64::try_from(offset) {
                    offset + self.length
                } else {
                    let offset = u64::try_from(-offset).unwrap();
                    self.length - offset
                }
            }
            SeekFrom::Current(offset) => {
                if let Ok(offset) = u64::try_from(offset) {
                    self.position + u64::try_from(self.buffer_write_position).to_io()? + offset
                } else {
                    let absolute_offset = -offset;
                    let offset = u64::try_from(absolute_offset).unwrap();
                    self.position + u64::try_from(self.buffer_write_position).to_io()? - offset
                }
            }
        };

        let buffer_len = u64::try_from(self.buffer.len()).unwrap();
        let new_position_in_buffer = match new_position.checked_sub(self.position) {
            Some(position) if position < buffer_len => Some(position),
            _ => None,
        };

        if let Some(new_position_in_buffer) = new_position_in_buffer {
            self.buffer_write_position = usize::try_from(new_position_in_buffer).to_io()?;
        } else {
            self.flush_buffer()?;
            self.file.seek(SeekFrom::Start(new_position))?;
            self.position = new_position;
        }

        Ok(new_position)
    }
}

pub trait Bufferable {
    fn len(&self) -> io::Result<u64>;
    fn set_len(&self, new_length: u64) -> io::Result<()>;
}

impl Bufferable for fs::File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn set_len(&self, new_length: u64) -> io::Result<()> {
        self.set_len(new_length)
    }
}

#[derive(Debug)]
pub struct WriteBuffer {
    pub bytes: Vec<u8>,
    pub position: u64,
}
