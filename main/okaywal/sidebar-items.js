window.SIDEBAR_ITEMS = {"enum":[["ReadChunkResult","The result of reading a chunk from a log segment."],["Recovery","Determines whether to recover a segment or not."]],"struct":[["ChunkReader","A buffered reader for a previously written data chunk."],["ChunkRecord","A record of a chunk that was written to a [`WriteAheadLog`]."],["Configuration","A [`WriteAheadLog`] configuration."],["Entry","A stored entry inside of a `WriteAheadLog`."],["EntryChunk","A chunk of data previously written using `EntryWriter::write_chunk`."],["EntryId","The unique id of an entry written to a [`WriteAheadLog`]. These IDs are ordered by the time the [`EntryWriter`] was created for the entry written with this id."],["EntryWriter","A writer for an entry in a [`WriteAheadLog`]."],["LogPosition","The position of a chunk of data within a [`WriteAheadLog`]."],["LogVoid","A [`LogManager`] that does not attempt to recover any existing data."],["RecoveredSegment","Information about an individual segment of a `WriteAheadLog` that is being recovered."],["SegmentReader","Reads a log segment, which contains one or more log entries."],["WriteAheadLog","A Write-Ahead Log that provides atomic and durable writes."]],"trait":[["LogManager","Customizes recovery and checkpointing behavior for a `WriteAheadLog`."]]};