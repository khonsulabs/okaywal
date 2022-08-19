var searchIndex = JSON.parse('{\
"okaywal":{"doc":"Okay WAL","t":[12,13,13,8,13,3,3,13,3,3,3,3,4,13,3,4,13,3,4,6,13,3,3,11,12,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,10,11,11,11,11,11,11,11,11,11,11,11,12,11,11,11,12,11,11,11,12,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,11,11,11,11,12,11,11,11,11,11,10,11,11,11,10,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,12,11,11,12,12],"n":["0","Abandon","AbortedEntry","Checkpointer","Chunk","ChunkRecord","Configuration","EndOfEntry","Entry","EntryChunk","EntryId","EntryWriter","Error","Io","LogPosition","ReadChunkResult","Recover","RecoveredSegment","Recovery","Result","ShuttingDown","VoidCheckpointer","WriteAheadLog","abandon","active_segment_limit","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","bytes_remaining","check_crc","checkpoint_after_bytes","checkpoint_to","checkpoint_to","clone","clone","clone","clone","clone_into","clone_into","clone_into","clone_into","cmp","commit","crc","default","default","drop","entry_id","eq","eq","eq","file_manager","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","fmt","from","from","from","from","from","from","from","from","from","from","from","from","from","from","id","into","into","into","into","into","into","into","into","into","into","into","into","into","length","ne","ne","ne","partial_cmp","position","read","read_all","read_all_chunks","read_at","read_chunk","recover","recover","recover","recover_with_config","should_recover_segment","should_recover_segment","shutdown","source","to_owned","to_owned","to_owned","to_owned","to_string","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","type_id","version_info","version_info","write","write_chunk","0","0"],"q":["okaywal","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","okaywal::Error","okaywal::ReadChunkResult"],"d":["","Abandon the segment and any entries stored within it. …","An aborted entry was detected. This should only be …","Customizes recovery and checkpointing behavior for a …","A chunk was found.","A record of a chunk that was written to a <code>WriteAheadLog</code>.","Controls a <code>WriteAheadLog</code>’s behavior.","The end of the entry has been reached.","A stored entry inside of a <code>WriteAheadLog</code>.","A chunk of data previously written using …","The unique id of an entry written to a <code>WriteAheadLog</code>. …","Writes an entry to a <code>WriteAheadLog</code>.","An error from a <code>WriteAheadLog</code>.","An unexpected IO error occurred.","The position of a chunk of data within a <code>WriteAheadLog</code>.","The result of reading a chunk from a log segment.","Recover the segment.","Information about an individual segment of a <code>WriteAheadLog</code> …","Determines whether to recover a segment or not.","A result from <code>okwal</code>.","The write-ahead log is shutting down.","A <code>Checkpointer</code> that does not attempt to recover any …","A Write-Ahead Log that enables atomic, durable writes.","Abandons this entry, preventing the entry from being …","The maximum number of segments that can have a <code>EntryWriter</code> …","","","","","","","","","","","","","","","","","","","","","","","","","","","Returns the number of bytes remaining to read from this …","Returns true if the CRC has been validated, or false if …","The number of bytes that need to be written before …","Invoked each time the <code>WriteAheadLog</code> is ready to recycle …","","","","","","","","","","","Commits this entry to the log. Once this call returns, all …","The crc calculated for the chunk.","","","","The unique id of the entry being written.","","","","The file manager to use for all file operations.","","","","","","","","","","","","","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","","The unique id of this entry.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","The length of the data contained inside of the chunk.","","","","","The position of the chunk.","","Reads all of the remaining data from this chunk.","Reads all chunks for this entry. If the entry was …","Opens the log to read previously written data.","Reads the next chunk of data written in this entry. If …","Invoked once for each entry contained in all recovered …","Creates or recovers a log stored in <code>directory</code> using the …","","Creates or recovers a log stored in <code>directory</code> using the …","When recovering a <code>WriteAheadLog</code>, this function is called …","","Shuts the log down. This prevents new writes or …","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","This information is written to the start of each log …","The value of <code>Configuration::version_info</code> at the time this …","Opens the log for writing. The entry written by the …","Appends a chunk of data to this log entry. Each chunk of …","",""],"i":[1,2,3,0,3,0,0,3,0,0,0,0,0,4,0,0,2,0,0,0,4,0,0,5,6,2,5,7,6,8,1,9,3,10,11,12,13,4,2,5,7,6,8,1,9,3,10,11,12,13,4,10,10,6,14,13,7,1,11,12,7,1,11,12,1,5,12,6,1,5,5,1,11,12,6,7,6,8,1,9,3,10,11,12,13,4,4,2,5,7,6,8,1,9,3,10,11,12,13,4,4,9,2,5,7,6,8,1,9,3,10,11,12,13,4,12,1,11,12,1,12,10,10,9,7,9,14,7,13,7,14,13,7,4,7,1,11,12,4,2,5,7,6,8,1,9,3,10,11,12,13,4,2,5,7,6,8,1,9,3,10,11,12,13,4,2,5,7,6,8,1,9,3,10,11,12,13,4,6,8,7,5,15,16],"f":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,[[["entrywriter",3]],["result",6]],null,[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["",0]],["",0]],[[["entrychunk",3]],["u32",0]],[[["entrychunk",3]],["option",4,[["bool",0]]]],null,[[["",0],["entryid",3]],["result",6]],[[["voidcheckpointer",3],["entryid",3]],["result",6]],[[["writeaheadlog",3]],["writeaheadlog",3]],[[["entryid",3]],["entryid",3]],[[["logposition",3]],["logposition",3]],[[["chunkrecord",3]],["chunkrecord",3]],[[["",0],["",0]]],[[["",0],["",0]]],[[["",0],["",0]]],[[["",0],["",0]]],[[["entryid",3],["entryid",3]],["ordering",4]],[[["entrywriter",3]],["result",6,[["entryid",3]]]],null,[[],["configuration",3]],[[],["entryid",3]],[[["entrywriter",3]]],null,[[["entryid",3],["entryid",3]],["bool",0]],[[["logposition",3],["logposition",3]],["bool",0]],[[["chunkrecord",3],["chunkrecord",3]],["bool",0]],null,[[["writeaheadlog",3],["formatter",3]],["result",6]],[[["configuration",3],["formatter",3]],["result",6]],[[["recoveredsegment",3],["formatter",3]],["result",6]],[[["entryid",3],["formatter",3]],["result",6]],[[["entry",3],["formatter",3]],["result",6]],[[["readchunkresult",4],["formatter",3]],["result",6]],[[["entrychunk",3],["formatter",3]],["result",6]],[[["logposition",3],["formatter",3]],["result",6]],[[["chunkrecord",3],["formatter",3]],["result",6]],[[["voidcheckpointer",3],["formatter",3]],["result",6]],[[["error",4],["formatter",3]],["result",6]],[[["error",4],["formatter",3]],["result",6]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[["error",3]],["error",4]],[[["entry",3]],["entryid",3]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],null,[[["entryid",3],["entryid",3]],["bool",0]],[[["logposition",3],["logposition",3]],["bool",0]],[[["chunkrecord",3],["chunkrecord",3]],["bool",0]],[[["entryid",3],["entryid",3]],["option",4,[["ordering",4]]]],null,[[["entrychunk",3]],["result",6,[["usize",0]]]],[[["entrychunk",3]],["result",6,[["vec",3,[["u8",0]]]]]],[[["entry",3]],["result",6,[["option",4,[["vec",3,[["vec",3,[["u8",0]]]]]]]]]],[[["writeaheadlog",3],["logposition",3]],["result",6,[["file",3]]]],[[["entry",3]],["result",6,[["readchunkresult",4]]]],[[["",0],["entry",3]],["result",6]],[[["asref",8,[["path",3]]],["checkpointer",8]],["result",6,[["writeaheadlog",3]]]],[[["voidcheckpointer",3],["entry",3]],["result",6]],[[["asref",8,[["path",3]]],["checkpointer",8],["configuration",3]],["result",6,[["writeaheadlog",3]]]],[[["",0],["recoveredsegment",3]],["result",6,[["recovery",4]]]],[[["voidcheckpointer",3],["recoveredsegment",3]],["result",6,[["recovery",4]]]],[[["writeaheadlog",3]],["result",6]],[[["error",4]],["option",4,[["error",8]]]],[[["",0]]],[[["",0]]],[[["",0]]],[[["",0]]],[[["",0]],["string",3]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[],["result",4]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],[[["",0]],["typeid",3]],null,null,[[["writeaheadlog",3]],["result",6,[["entrywriter",3]]]],[[["entrywriter",3]],["result",6,[["chunkrecord",3]]]],null,null],"p":[[3,"EntryId"],[4,"Recovery"],[4,"ReadChunkResult"],[4,"Error"],[3,"EntryWriter"],[3,"Configuration"],[3,"WriteAheadLog"],[3,"RecoveredSegment"],[3,"Entry"],[3,"EntryChunk"],[3,"LogPosition"],[3,"ChunkRecord"],[3,"VoidCheckpointer"],[8,"Checkpointer"],[13,"Io"],[13,"Chunk"]]}\
}');
if (typeof window !== 'undefined' && window.initSearch) {window.initSearch(searchIndex)};
if (typeof exports !== 'undefined') {exports.searchIndex = searchIndex};
