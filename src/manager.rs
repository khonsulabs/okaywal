use std::{fmt::Debug, io};

use file_manager::{fs::StdFileManager, FileManager};

use crate::{
    entry::EntryId,
    log_file::{Entry, RecoveredSegment, SegmentReader},
    WriteAheadLog,
};

/// Customizes recovery and checkpointing behavior for a
/// [`WriteAheadLog`](crate::WriteAheadLog).
pub trait LogManager<M = StdFileManager>: Send + Sync + Debug + 'static
where
    M: FileManager,
{
    /// When recovering a [`WriteAheadLog`](crate::WriteAheadLog), this function
    /// is called for each segment as it is read. To allow the segment to have
    /// its data recovered, return [`Recovery::Recover`]. If you wish to abandon
    /// the data contained in the segment, return [`Recovery::Abandon`].
    fn should_recover_segment(&mut self, _segment: &RecoveredSegment) -> io::Result<Recovery> {
        Ok(Recovery::Recover)
    }

    /// Invoked once for each entry contained in all recovered segments within a
    /// [`WriteAheadLog`](crate::WriteAheadLog).
    ///
    /// [`Entry::read_chunk()`] can be used to read each chunk of data that was
    /// written via
    /// [`EntryWriter::write_chunk`](crate::EntryWriter::write_chunk). The order
    /// of chunks is guaranteed to be the same as the order they were written
    /// in.
    fn recover(&mut self, entry: &mut Entry<'_, M::File>) -> io::Result<()>;

    /// Invoked each time the [`WriteAheadLog`](crate::WriteAheadLog) is ready
    /// to recycle and reuse segment files.
    ///
    /// `last_checkpointed_id` is the id of the last entry that is being
    /// checkedpointed and removed from the log. If needed,
    /// `checkpointed_entries` can be used to iterate over all entries that are
    /// being checkpointed.
    ///
    /// Shortly after function returns, the entries stored within the file being
    /// checkpointed will no longer be accessible. To ensure ACID compliance of
    /// the underlying storage layer, all necessary changes must be fully
    /// synchronized to the underlying storage medium before this function call
    /// returns.
    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: EntryId,
        checkpointed_entries: &mut SegmentReader<M::File>,
        wal: &WriteAheadLog<M>,
    ) -> io::Result<()>;
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

/// A [`LogManager`] that does not attempt to recover any existing data.
#[derive(Debug)]
pub struct LogVoid;

impl<M> LogManager<M> for LogVoid
where
    M: file_manager::FileManager,
{
    fn should_recover_segment(&mut self, _segment: &RecoveredSegment) -> io::Result<Recovery> {
        Ok(Recovery::Abandon)
    }

    fn recover(&mut self, _entry: &mut Entry<'_, M::File>) -> io::Result<()> {
        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        _last_checkpointed_id: EntryId,
        _reader: &mut SegmentReader<M::File>,
        _wal: &WriteAheadLog<M>,
    ) -> io::Result<()> {
        Ok(())
    }
}
