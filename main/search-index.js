var searchIndex = JSON.parse('{\
"okaywal":{"doc":"A write-ahead log (WAL) implementation for Rust.","t":[12,13,13,13,3,3,3,13,3,3,3,3,8,3,3,4,13,3,4,3,3,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,11,11,11,11,12,10,11,11,11,11,11,11,11,11,11,11,11,11,12,11,11,11,11,12,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,11,11,11,12,11,12,11,11,11,11,11,11,11,10,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,12,11,12],"n":["0","Abandon","AbortedEntry","Chunk","ChunkReader","ChunkRecord","Configuration","EndOfEntry","Entry","EntryChunk","EntryId","EntryWriter","LogManager","LogPosition","LogVoid","ReadChunkResult","Recover","RecoveredSegment","Recovery","SegmentReader","WriteAheadLog","begin_chunk","begin_entry","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","buffer_bytes","buffer_bytes","bytes_remaining","bytes_remaining","check_crc","checkpoint_after_bytes","checkpoint_after_bytes","checkpoint_to","checkpoint_to","chunk_length","clone","clone","clone","clone","clone_into","clone_into","clone_into","clone_into","cmp","commit","crc","crc_is_valid","default","default","default_for","directory","drop","drop","eq","eq","eq","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","from","from","from","from","from","from","from","from","from","from","from","from","from","from","id","id","into","into","into","into","into","into","into","into","into","into","into","into","into","into","length","log_position","open","partial_cmp","position","preallocate_bytes","preallocate_bytes","read","read","read_all","read_all_chunks","read_at","read_chunk","read_entry","recover","recover","recover","rollback","segment","should_recover_segment","should_recover_segment","should_recover_segment","shutdown","skip_remaining_bytes","to_owned","to_owned","to_owned","to_owned","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","version_info","version_info","write_chunk","0"],"q":["okaywal","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","okaywal::ReadChunkResult"],"d":["","Abandon the segment and any entries stored within it. …","An aborted entry was detected. This should only be …","A chunk was found.","A buffered reader for a previously written data chunk.","A record of a chunk that was written to a <code>WriteAheadLog</code>.","A <code>WriteAheadLog</code> configuration.","The end of the entry has been reached.","A stored entry inside of a <code>WriteAheadLog</code>.","A chunk of data previously written using …","The unique id of an entry written to a <code>WriteAheadLog</code>. …","A writer for an entry in a <code>WriteAheadLog</code>.","Customizes recovery and checkpointing behavior for a …","The position of a chunk of data within a <code>WriteAheadLog</code>.","A <code>LogManager</code> that does not attempt to recover any existing …","The result of reading a chunk from a log segment.","Recover the segment.","Information about an individual segment of a <code>WriteAheadLog</code> …","Determines whether to recover a segment or not.","Reads a log segment, which contains one or more log …","A Write-Ahead Log that provides atomic and durable writes.","Begins writing a chunk with the given <code>length</code>.","Begins writing an entry to this log.","","","","","","","","","","","","","","","","","","","","","","","","","","","","","Sets the number of bytes to use for internal buffers when …","The number of bytes to use for the in-memory buffer when …","Returns the number of bytes remaining to read from this …","Returns the number of bytes remaining to read.","Returns true if the CRC has been validated, or false if …","Sets the number of bytes written required to begin a …","After this many bytes have been written to the active log …","Invoked each time the <code>WriteAheadLog</code> is ready to recycle …","","Returns the length of the data stored.","","","","","","","","","","Commits this entry to the log. Once this call returns, all …","The crc calculated for the chunk.","Returns true if the stored checksum matches the computed …","","","Returns the default configuration for a given directory.","The directory to store the log files in.","","","","","","","","","","","","","","","","","","","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the unique id of the log entry being written.","The unique id of this entry.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","The length of the data contained inside of the chunk.","Returns the position that this chunk is located at.","Opens the log using the provided log manager with this …","","The position of the chunk.","Sets the number of bytes to preallocate for each segment …","The number of bytes each log file should be preallocated …","","","Reads all of the remaining data from this chunk.","Reads all chunks for this entry. If the entry was …","Opens the log to read previously written data.","Reads the next chunk of data written in this entry. If …","Reads an entry from the log. If no more entries are found, …","Invoked once for each entry contained in all recovered …","","Creates or opens a log in <code>directory</code> using the provided log …","Abandons this entry, preventing the entry from being …","The segment that this entry was recovered from.","When recovering a <code>WriteAheadLog</code>, this function is called …","When recovering a <code>WriteAheadLog</code>, this function is called …","","Waits for all other instances of <code>WriteAheadLog</code> to be …","Advances past the end of this chunk without reading the …","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","An arbitrary chunk of bytes that is stored in the log …","The value of <code>Configuration::version_info</code> at the time this …","Appends a chunk of data to this log entry. Each chunk of …",""],"i":[11,28,22,22,0,0,0,22,0,0,0,0,0,0,0,0,28,0,0,0,0,1,4,28,5,1,14,15,11,12,21,22,7,23,13,4,8,28,5,1,14,15,11,12,21,22,7,23,13,4,8,5,5,7,8,7,5,5,24,13,8,14,15,11,4,14,15,11,4,11,1,15,8,5,11,5,5,1,7,14,15,11,5,1,14,15,11,12,21,22,7,23,13,4,8,28,5,1,14,15,11,12,21,22,7,23,13,4,8,1,21,28,5,1,14,15,11,12,21,22,7,23,13,4,8,15,7,5,11,15,5,5,7,8,7,21,4,21,12,24,13,4,1,21,24,24,13,4,7,14,15,11,4,28,5,1,14,15,11,12,21,22,7,23,13,4,8,28,5,1,14,15,11,12,21,22,7,23,13,4,8,28,5,1,14,15,11,12,21,22,7,23,13,4,8,5,23,1,31],"f":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,[[1,2],[[3,[0]]]],[4,[[3,[1]]]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[5,6],5],0,[7,2],[8,2],[7,[[3,[9]]]],[[5,10],5],0,[[11,12],3],[[13,11,12],3],[8,2],[14,14],[15,15],[11,11],[4,4],[[]],[[]],[[]],[[]],[[11,11],16],[1,[[3,[11]]]],0,[8,[[3,[9]]]],[[],5],[[],11],[[[18,[17]]],5],0,[1],[7],[[14,14],9],[[15,15],9],[[11,11],9],[[5,19],20],[[1,19],20],[[14,19],20],[[15,19],20],[[11,19],20],[[12,19],20],[[21,19],20],[[22,19],20],[[7,19],20],[[23,19],20],[[13,19],20],[[4,19],20],[[8,19],20],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[1,11],[21,11],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],0,[7,14],[[5,24],[[3,[4]]]],[[11,11],[[25,[16]]]],0,[[5,2],5],0,[7,[[3,[6]]]],[8,[[3,[6]]]],[7,[[3,[[27,[26]]]]]],[21,[[3,[[25,[[27,[[27,[26]]]]]]]]]],[[4,14],[[3,[8]]]],[21,[[3,[22]]]],[12,[[3,[[25,[21]]]]]],[21,3],[[13,21],3],[[[18,[17]],24],[[3,[4]]]],[1,3],[21,23],[23,[[3,[28]]]],[23,[[3,[28]]]],[[13,23],[[3,[28]]]],[4,3],[7,3],[[]],[[]],[[]],[[]],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],29],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],[[],30],0,0,[1,[[3,[15]]]],0],"p":[[3,"EntryWriter"],[15,"u32"],[6,"Result"],[3,"WriteAheadLog"],[3,"Configuration"],[15,"usize"],[3,"EntryChunk"],[3,"ChunkReader"],[15,"bool"],[15,"u64"],[3,"EntryId"],[3,"SegmentReader"],[3,"LogVoid"],[3,"LogPosition"],[3,"ChunkRecord"],[4,"Ordering"],[3,"Path"],[8,"AsRef"],[3,"Formatter"],[6,"Result"],[3,"Entry"],[4,"ReadChunkResult"],[3,"RecoveredSegment"],[8,"LogManager"],[4,"Option"],[15,"u8"],[3,"Vec"],[4,"Recovery"],[4,"Result"],[3,"TypeId"],[13,"Chunk"]]}\
}');
if (typeof window !== 'undefined' && window.initSearch) {window.initSearch(searchIndex)};
if (typeof exports !== 'undefined') {exports.searchIndex = searchIndex};
