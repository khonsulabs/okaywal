use std::{io, num::TryFromIntError};

pub trait ToIoResult<T> {
    fn to_io(self) -> io::Result<T>;
}

impl<T> ToIoResult<T> for std::result::Result<T, TryFromIntError> {
    fn to_io(self) -> io::Result<T> {
        match self {
            Ok(result) => Ok(result),
            Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
        }
    }
}

impl<T, U> ToIoResult<T> for Result<T, flume::SendError<U>> {
    fn to_io(self) -> io::Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "sender disconnected",
            )),
        }
    }
}
