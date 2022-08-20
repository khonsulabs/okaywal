window.SIDEBAR_ITEMS = {"enum":[["Error","An error from a [`WriteAheadLog`]."],["ReadChunkResult","The result of reading a chunk from a log segment."],["Recovery","Determines whether to recover a segment or not."]],"struct":[["ChunkRecord","A record of a chunk that was written to a [`WriteAheadLog`]."],["Configuration","Controls a [`WriteAheadLog`]’s behavior."],["Entry","A stored entry inside of a [`WriteAheadLog`]."],["EntryChunk","A chunk of data previously written using [`EntryWriter::write_chunk`]."],["EntryId","The unique id of an entry written to a [`WriteAheadLog`]. These IDs are ordered by the time the [`EntryWriter`] was created for the entry written with this id."],["EntryWriter","Writes an entry to a [`WriteAheadLog`]."],["File","An open file."],["LockedFile","An open file that has an exclusive file lock."],["LogPosition","The position of a chunk of data within a [`WriteAheadLog`]."],["LogReader","Reads a collection of [`Entry`]s from a [`WriteAheadLog`]."],["RecoveredSegment","Information about an individual segment of a [`WriteAheadLog`] that is being recovered."],["VoidCheckpointer","A [`Checkpointer`] that does not attempt to recover any existing data."],["WriteAheadLog","A Write-Ahead Log that enables atomic, durable writes."]],"trait":[["Checkpointer","Customizes recovery and checkpointing behavior for a [`WriteAheadLog`]."]],"type":[["Result","A result from `okwal`."]]};