use std::{fmt::Debug, io};

use crate::{
    entry::EntryId,
    log_file::{Entry, RecoveredSegment, SegmentReader},
};

/// Customizes recovery and checkpointing behavior for a [`WriteAheadLog`].
pub trait LogManager: Send + Sync + Debug + 'static {
    /// When recovering a [`WriteAheadLog`], this function is called for each
    /// segment as it is read. To allow the segment to have its data recovered,
    /// return [`Recovery::Recover`]. If you wish to abandon the data contained
    /// in the segment, return [`Recovery::Abandon`].
    fn should_recover_segment(&mut self, _segment: &RecoveredSegment) -> io::Result<Recovery> {
        Ok(Recovery::Recover)
    }

    /// Invoked once for each entry contained in all recovered segments within a
    /// [`WriteAheadLog`].
    ///
    /// [`Entry::read_chunk()`] can be used to read each chunk of data that was
    /// written via [`EntryWriter::write_chunk`]. The order of chunks is guaranteed
    /// to be the same as the order they were written in.
    fn recover(&mut self, entry: &mut Entry<'_>) -> io::Result<()>;

    /// Invoked each time the [`WriteAheadLog`] is ready to recycle and reuse
    /// segment files.
    ///
    /// `last_checkpointed_id` is the id of the last entry that is being
    /// checkedpointed and removed from the log. If needed,
    /// `checkpointed_entries` can be used to iterate over all entries that are
    /// being checkpointed.
    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: EntryId,
        checkpointed_entries: &mut SegmentReader,
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

impl LogManager for LogVoid {
    fn should_recover_segment(&mut self, _segment: &RecoveredSegment) -> io::Result<Recovery> {
        Ok(Recovery::Abandon)
    }

    fn recover(&mut self, _entry: &mut Entry<'_>) -> io::Result<()> {
        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        _last_checkpointed_id: EntryId,
        _reader: &mut SegmentReader,
    ) -> io::Result<()> {
        Ok(())
    }
}
