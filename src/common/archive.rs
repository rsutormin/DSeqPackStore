// Archive I/O
// Custom binary format for storing compressed genome data in streams/parts

use crate::common::varint::{read_varint, write_varint};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

const EXTERNAL_PACK: u64 = u64::MAX;

fn block_directory(path: &Path) -> PathBuf {
    path.parent().unwrap_or_else(|| Path::new(".")).join("blocks")
}

fn pack_descriptor(data: &[u8]) -> Result<(&str, u64)> {
    let length = u64::from_le_bytes(data.get(..8).context("Missing external pack length")?.try_into()?);
    let name = std::str::from_utf8(&data[8..])?;
    let stem = name.strip_suffix(".zst").context("Invalid external pack extension")?;
    anyhow::ensure!(!stem.is_empty() && stem.bytes().all(|byte| byte.is_ascii_hexdigit() || byte == b'-'),
        "Invalid external pack name");
    Ok((name, length))
}

/// Synchronize archive data using the publication mode selected by the benchmark.
pub fn sync_file(file: &File) -> std::io::Result<()> {
    if std::env::var_os("RAGC_BENCH_SYNC").is_some_and(|mode| mode == "fsync") {
        extern "C" { fn fsync(fd: std::ffi::c_int) -> std::ffi::c_int; }
        // The File keeps the descriptor open during the call.
        if unsafe { fsync(file.as_raw_fd()) } == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    } else {
        file.sync_all()
    }
}

/// A part within a stream (offset and size in file)
#[derive(Debug, Clone)]
struct Part {
    offset: u64,
    size: u64,
}

impl Part {
    fn new(offset: u64, size: u64) -> Self {
        Part { offset, size }
    }
}

/// A stream containing multiple parts
#[derive(Debug)]
struct Stream {
    stream_name: String,
    cur_id: usize,
    raw_size: u64,
    packed_size: u64,
    packed_data_size: u64,
    parts: Vec<Part>,
}

impl Stream {
    fn new(stream_name: String) -> Self {
        Stream {
            stream_name,
            cur_id: 0,
            raw_size: 0,
            packed_size: 0,
            packed_data_size: 0,
            parts: Vec::new(),
        }
    }
}

/// Archive for reading/writing AGC data
pub struct Archive {
    input_mode: bool,
    block_dir: Option<PathBuf>,
    file: Option<File>,
    reader: Option<BufReader<File>>,
    writer: Option<BufWriter<File>>,
    f_offset: u64,
    previous: Option<Box<Archive>>,
    streams: Vec<Stream>,
    stream_map: HashMap<String, usize>,
    /// Write buffer for parallel Phase 3 (C++ AGC: m_buffer)
    /// Maps stream_id -> Vec of (data, metadata) tuples
    /// BTreeMap ensures writes are flushed in sorted stream_id order for determinism
    write_buffer: BTreeMap<usize, Vec<(Vec<u8>, u64)>>,
}

impl Archive {
    /// Create a new archive in input (read) mode
    pub fn new_reader() -> Self {
        Archive {
            input_mode: true,
            block_dir: None,
            file: None,
            reader: None,
            writer: None,
            f_offset: 0,
            previous: None,
            streams: Vec::new(),
            stream_map: HashMap::new(),
            write_buffer: BTreeMap::new(),
        }
    }

    /// Create a new archive in output (write) mode
    pub fn new_writer() -> Self {
        Archive {
            input_mode: false,
            block_dir: None,
            file: None,
            reader: None,
            writer: None,
            f_offset: 0,
            previous: None,
            streams: Vec::new(),
            stream_map: HashMap::new(),
            write_buffer: BTreeMap::new(),
        }
    }

    /// Open an archive file
    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        self.block_dir = Some(block_directory(path.as_ref()));
        if self.input_mode {
            let file = File::open(path).context("Failed to open archive for reading")?;
            self.reader = Some(BufReader::new(file.try_clone()?));
            self.file = Some(file);
            self.deserialize()?;
        } else {
            let file = File::create(path).context("Failed to create archive for writing")?;
            // Use 4MB buffer to batch writes and minimize syscalls
            // Default 8KB buffer causes ~200 syscalls for typical archives
            self.writer = Some(BufWriter::with_capacity(4 * 1024 * 1024, file.try_clone()?));
            self.file = Some(file);
        }
        self.f_offset = 0;
        Ok(())
    }

    /// Write an archive suffix with absolute block offsets into the previous file.
    pub fn open_suffix(&mut self, path: &Path, source: &Path) -> Result<()> {
        anyhow::ensure!(!self.input_mode && self.writer.is_none(), "Expected a fresh writer");
        let mut previous = Self::new_reader();
        previous.open(source)?;
        let offset = previous.file.as_ref().context("Missing source file")?.metadata()?.len();
        let file = File::options().write(true).create_new(true).open(path)?;
        self.writer = Some(BufWriter::with_capacity(4 * 1024 * 1024, file));
        self.previous = Some(Box::new(previous));
        self.block_dir = Some(block_directory(path));
        self.f_offset = offset;
        Ok(())
    }

    /// Reference a part already present in the source archive.
    pub fn reuse_part(&mut self, stream_id: usize, part_id: usize) -> Result<()> {
        let stream = &mut self.streams[stream_id];
        let previous = self.previous.as_ref().context("Missing source archive")?;
        let from = previous.get_stream_id(&stream.stream_name).context("Missing source stream")?;
        let part = previous.streams[from].parts.get(part_id).context("Missing source part")?;
        stream.parts.push(part.clone());
        Ok(())
    }

    /// Append one independent frame to an open pack and publish its new byte bound.
    pub fn append_external_pack(
        &mut self, stream_id: usize, resume: bool, frame: &[u8],
    ) -> Result<()> {
        let stream = &self.streams[stream_id];
        let ordinal = stream.parts.len();
        let stream_name: String = stream.stream_name.bytes().map(|byte| format!("{byte:02x}")).collect();
        let name = format!("{stream_name}-{ordinal}.zst");
        let previous_length = if resume {
            let old = self.previous.as_mut().context("Missing previous archive")?;
            let id = old.get_stream_id(&stream.stream_name).context("Missing previous external stream")?;
            let part = old.streams[id].parts.get(ordinal).context("Missing previous external pack")?.clone();
            let (data, metadata) = old.read_physical_part(&part)?.context("Missing previous descriptor")?;
            anyhow::ensure!(metadata == EXTERNAL_PACK, "Previous pack is not external");
            let (old_name, previous_length) = pack_descriptor(&data)?;
            anyhow::ensure!(old_name == name, "External pack identity changed");
            if frame.is_empty() {
                return self.reuse_part(stream_id, ordinal);
            }
            previous_length
        } else {
            anyhow::ensure!(!frame.is_empty(), "New external pack requires a frame");
            0
        };
        let path = self.block_dir.as_ref().context("Missing block directory")?.join(&name);
        let mut file = File::options().write(true).create(true).truncate(false).open(path)?;
        let file_len = file.metadata()?.len();
        anyhow::ensure!(file_len >= previous_length, "External pack is shorter than the committed length");
        // A failed publication can leave uncommitted bytes after the previous snapshot's bound.
        if file_len > previous_length {
            file.set_len(previous_length)?;
        }
        file.seek(SeekFrom::Start(previous_length))?;
        file.write_all(frame)?;
        sync_file(&file)?;
        let mut descriptor = (previous_length + frame.len() as u64).to_le_bytes().to_vec();
        descriptor.extend_from_slice(name.as_bytes());
        self.add_part(stream_id, &descriptor, EXTERNAL_PACK)
    }

    /// Close the archive (writes footer in write mode)
    pub fn close(&mut self) -> Result<()> {
        if !self.input_mode {
            if let Some(ref mut writer) = self.writer {
                writer.flush()?;
            }
            self.serialize()?;
        }

        self.reader = None;
        self.writer = None;
        self.file = None;
        Ok(())
    }

    /// Register a new stream and return its ID
    pub fn register_stream(&mut self, stream_name: &str) -> usize {
        // Check if already registered
        if let Some(&id) = self.stream_map.get(stream_name) {
            return id;
        }

        let id = self.streams.len();
        self.streams.push(Stream::new(stream_name.to_string()));
        self.stream_map.insert(stream_name.to_string(), id);
        id
    }

    /// Get stream ID by name (returns None if not found)
    pub fn get_stream_id(&self, stream_name: &str) -> Option<usize> {
        self.stream_map.get(stream_name).copied()
    }

    /// Get list of all stream names
    pub fn get_stream_names(&self) -> Vec<String> {
        self.streams.iter().map(|s| s.stream_name.clone()).collect()
    }

    /// Add a part to a stream
    pub fn add_part(&mut self, stream_id: usize, data: &[u8], metadata: u64) -> Result<()> {
        if stream_id >= self.streams.len() {
            anyhow::bail!("Invalid stream ID: {stream_id}");
        }

        let stream = &self.streams[stream_id];
        if let Some(previous) = self.previous.as_mut() {
            if let Some(old_stream) = previous.get_stream_id(&stream.stream_name) {
                let part = previous.streams[old_stream].parts.get(stream.parts.len()).cloned();
                if let Some(part) = part.filter(|part| part.size == data.len() as u64) {
                    let (old_data, old_metadata) = previous.read_physical_part(&part)?.context("Missing source data")?;
                    if old_metadata == metadata && old_data == data {
                        self.streams[stream_id].parts.push(part);
                        return Ok(());
                    }
                }
            }
        }
        let writer = self
            .writer
            .as_mut()
            .context("Archive not open for writing")?;

        // Record part offset (before writing anything)
        let part_offset = self.f_offset;

        // Write metadata as varint
        let mut metadata_buf = Vec::new();
        write_varint(&mut metadata_buf, metadata)?;
        writer.write_all(&metadata_buf)?;
        self.f_offset += metadata_buf.len() as u64;

        // Write data
        writer.write_all(data)?;
        self.f_offset += data.len() as u64;

        // Record part (size is only the data size, not including metadata)
        self.streams[stream_id]
            .parts
            .push(Part::new(part_offset, data.len() as u64));

        // packed_size includes both metadata and data
        let total_size = self.f_offset - part_offset;
        self.streams[stream_id].packed_size += total_size;
        self.streams[stream_id].packed_data_size += data.len() as u64;

        Ok(())
    }

    /// Buffer a part for later writing (C++ AGC: AddPartBuffered)
    /// This allows parallel compression while maintaining deterministic write order.
    /// Call flush_buffers() after all parts are buffered to write in sorted stream_id order.
    pub fn add_part_buffered(&mut self, stream_id: usize, data: Vec<u8>, metadata: u64) {
        self.write_buffer
            .entry(stream_id)
            .or_default()
            .push((data, metadata));
    }

    /// Flush all buffered parts to disk in sorted stream_id order (C++ AGC: flush_out_buffers)
    /// This ensures deterministic output regardless of which thread buffered the data.
    pub fn flush_buffers(&mut self) -> Result<()> {
        // BTreeMap iterates in sorted key order (stream_id)
        // This matches C++ AGC's flush_out_buffers() which iterates m_buffer (std::map)
        let buffer = std::mem::take(&mut self.write_buffer);
        for (stream_id, parts) in buffer {
            for (data, metadata) in parts {
                self.add_part(stream_id, &data, metadata)?;
            }
        }
        Ok(())
    }

    /// Set raw (uncompressed) size for a stream
    pub fn set_raw_size(&mut self, stream_id: usize, raw_size: u64) {
        if stream_id < self.streams.len() {
            self.streams[stream_id].raw_size = raw_size;
        }
    }

    /// Get raw size for a stream
    pub fn get_raw_size(&self, stream_id: usize) -> u64 {
        if stream_id < self.streams.len() {
            self.streams[stream_id].raw_size
        } else {
            0
        }
    }

    /// Get packed (compressed) size for a stream
    pub fn get_packed_size(&self, stream_id: usize) -> u64 {
        if stream_id < self.streams.len() {
            self.streams[stream_id].packed_size
        } else {
            0
        }
    }

    /// Get packed data size (compressed data only, without metadata)
    pub fn get_packed_data_size(&self, stream_id: usize) -> u64 {
        if stream_id < self.streams.len() {
            self.streams[stream_id].packed_data_size
        } else {
            0
        }
    }

    /// Get stream name by ID
    pub fn get_stream_name(&self, stream_id: usize) -> Option<&str> {
        if stream_id < self.streams.len() {
            Some(&self.streams[stream_id].stream_name)
        } else {
            None
        }
    }

    /// Get number of streams
    pub fn get_num_streams(&self) -> usize {
        self.streams.len()
    }

    /// Get number of parts in a stream
    pub fn get_num_parts(&self, stream_id: usize) -> usize {
        if stream_id < self.streams.len() {
            self.streams[stream_id].parts.len()
        } else {
            0
        }
    }

    /// Get the next part from a stream (sequential reading)
    pub fn get_part(&mut self, stream_id: usize) -> Result<Option<(Vec<u8>, u64)>> {
        if stream_id >= self.streams.len() {
            anyhow::bail!("Invalid stream ID: {stream_id}");
        }

        let stream = &mut self.streams[stream_id];
        if stream.cur_id >= stream.parts.len() {
            return Ok(None); // No more parts
        }

        let part = stream.parts[stream.cur_id].clone();
        stream.cur_id += 1;

        self.read_part_data(&part)
    }

    /// Get a specific part by ID from a stream (random access)
    pub fn get_part_by_id(&mut self, stream_id: usize, part_id: usize) -> Result<(Vec<u8>, u64)> {
        if stream_id >= self.streams.len() {
            anyhow::bail!("Invalid stream ID: {stream_id}");
        }

        let stream = &self.streams[stream_id];
        if part_id >= stream.parts.len() {
            anyhow::bail!("Invalid part ID: {part_id}");
        }

        let part = stream.parts[part_id].clone();
        self.read_part_data(&part)
            .map(|opt| opt.expect("Part should exist"))
    }

    /// Read part data from file
    fn read_part_data(&mut self, part: &Part) -> Result<Option<(Vec<u8>, u64)>> {
        let (data, metadata) = self.read_physical_part(part)?.context("Missing archive part")?;
        if metadata != EXTERNAL_PACK {
            return Ok(Some((data, metadata)));
        }
        let (name, length) = pack_descriptor(&data)?;
        let path = self.block_dir.as_ref().context("Missing block directory")?.join(name);
        let file = File::open(path)?;
        anyhow::ensure!(file.metadata()?.len() >= length, "External pack is truncated");
        let plain = zstd::decode_all(file.take(length))?;
        Ok(Some((plain, 0)))
    }

    fn read_physical_part(&mut self, part: &Part) -> Result<Option<(Vec<u8>, u64)>> {
        if part.size == 0 {
            return Ok(Some((Vec::new(), 0)));
        }

        let reader = self
            .reader
            .as_mut()
            .context("Archive not open for reading")?;

        // Seek to part offset
        reader.seek(SeekFrom::Start(part.offset))?;

        // Read metadata
        let (metadata, _) = read_varint(reader)?;

        // Read data (part.size is the data size, not including metadata)
        let mut data = vec![0u8; part.size as usize];
        reader.read_exact(&mut data)?;

        Ok(Some((data, metadata)))
    }

    /// Serialize footer to file (write mode)
    fn serialize(&mut self) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .context("Archive not open for writing")?;

        let mut footer = Vec::new();

        // Write number of streams
        write_varint(&mut footer, self.streams.len() as u64)?;

        // Write each stream's metadata
        for stream in &mut self.streams {
            // Stream name (null-terminated string)
            footer.extend_from_slice(stream.stream_name.as_bytes());
            footer.push(0);

            // Number of parts
            write_varint(&mut footer, stream.parts.len() as u64)?;

            // Raw size
            write_varint(&mut footer, stream.raw_size)?;

            // Part offsets and sizes
            for part in &stream.parts {
                write_varint(&mut footer, part.offset)?;
                write_varint(&mut footer, part.size)?;
            }

            // Update packed size to include footer overhead
            stream.packed_size += footer.len() as u64;
        }

        // Write footer
        writer.write_all(&footer)?;

        // Write footer size as fixed 8-byte value
        let footer_size = footer.len() as u64;
        writer.write_all(&footer_size.to_le_bytes())?;

        writer.flush()?;
        Ok(())
    }

    /// Deserialize footer from file (read mode)
    fn deserialize(&mut self) -> Result<()> {
        let file = self.file.as_mut().context("Archive not open")?;

        // Get file size
        let file_size = file.metadata()?.len();

        // Read footer size (last 8 bytes)
        file.seek(SeekFrom::End(-8))?;
        let mut footer_size_bytes = [0u8; 8];
        file.read_exact(&mut footer_size_bytes)?;
        let footer_size = u64::from_le_bytes(footer_size_bytes);

        // Seek to start of footer
        file.seek(SeekFrom::Start(file_size - 8 - footer_size))?;

        // Read footer into buffer
        let mut footer = vec![0u8; footer_size as usize];
        file.read_exact(&mut footer)?;

        // Parse footer
        let mut cursor = std::io::Cursor::new(&footer);

        // Read number of streams
        let (num_streams, _) = read_varint(&mut cursor)?;

        self.streams.clear();
        self.stream_map.clear();

        for i in 0..num_streams {
            // Read stream name (null-terminated)
            let mut stream_name = String::new();
            loop {
                let mut byte = [0u8; 1];
                cursor.read_exact(&mut byte)?;
                if byte[0] == 0 {
                    break;
                }
                stream_name.push(byte[0] as char);
            }

            // Read number of parts
            let (num_parts, _) = read_varint(&mut cursor)?;

            // Read raw size
            let (raw_size, _) = read_varint(&mut cursor)?;

            // Create stream
            let mut stream = Stream::new(stream_name.clone());
            stream.raw_size = raw_size;

            // Read parts
            for _ in 0..num_parts {
                let (offset, _) = read_varint(&mut cursor)?;
                let (size, _) = read_varint(&mut cursor)?;
                stream.parts.push(Part::new(offset, size));
            }

            // Reset cur_id for reading
            stream.cur_id = 0;

            self.streams.push(stream);
            self.stream_map.insert(stream_name, i as usize);
        }

        // Seek back to beginning for reading parts
        file.seek(SeekFrom::Start(0))?;
        if let Some(ref mut reader) = self.reader {
            reader.seek(SeekFrom::Start(0))?;
        }

        Ok(())
    }
}

impl Drop for Archive {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_archive_write_read() {
        let path = "test_archive.agc";

        // Write
        {
            let mut archive = Archive::new_writer();
            archive.open(path).unwrap();

            let stream_id = archive.register_stream("test_stream");
            archive.add_part(stream_id, b"Hello", 42).unwrap();
            archive.add_part(stream_id, b"World", 99).unwrap();
            archive.set_raw_size(stream_id, 100);

            archive.close().unwrap();
        }

        // Read
        {
            let mut archive = Archive::new_reader();
            archive.open(path).unwrap();

            let stream_id = archive.get_stream_id("test_stream").unwrap();
            assert_eq!(archive.get_num_parts(stream_id), 2);
            assert_eq!(archive.get_raw_size(stream_id), 100);

            let (data1, meta1) = archive.get_part(stream_id).unwrap().unwrap();
            assert_eq!(data1, b"Hello");
            assert_eq!(meta1, 42);

            let (data2, meta2) = archive.get_part(stream_id).unwrap().unwrap();
            assert_eq!(data2, b"World");
            assert_eq!(meta2, 99);

            assert!(archive.get_part(stream_id).unwrap().is_none());
        }

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn external_pack_preserves_snapshot_bounds_on_retry() {
        let root = std::env::temp_dir().join(format!("ragc-external-pack-test-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("blocks")).unwrap();
        let base = root.join("base.agc");
        let mut first = Archive::new_writer();
        first.open(&base).unwrap();
        let first_stream = first.register_stream("x1d");
        let first_frame = zstd::encode_all(&b"first"[..], 1).unwrap();
        first.append_external_pack(first_stream, false, &first_frame).unwrap();
        first.close().unwrap();

        let block = fs::read_dir(root.join("blocks")).unwrap().next().unwrap().unwrap().path();
        File::options().append(true).open(&block).unwrap().write_all(b"unpublished tail").unwrap();

        let suffix = root.join("trial.delta");
        let mut second = Archive::new_writer();
        second.open_suffix(&suffix, &base).unwrap();
        let second_stream = second.register_stream("x1d");
        let second_frame = zstd::encode_all(&b"second"[..], 1).unwrap();
        second.append_external_pack(second_stream, true, &second_frame).unwrap();
        second.close().unwrap();
        assert_eq!(fs::read(&block).unwrap(), [first_frame, second_frame].concat());

        let combined = root.join("combined.agc");
        fs::write(&combined, [fs::read(&base).unwrap(), fs::read(&suffix).unwrap()].concat()).unwrap();
        for (path, expected) in [(&base, &b"first"[..]), (&combined, &b"firstsecond"[..])] {
            let mut reader = Archive::new_reader();
            reader.open(path).unwrap();
            let stream = reader.get_stream_id("x1d").unwrap();
            assert_eq!(reader.get_part_by_id(stream, 0).unwrap(), (expected.to_vec(), 0));
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn test_multiple_streams() {
        let path = "test_multi_stream.agc";

        // Write
        {
            let mut archive = Archive::new_writer();
            archive.open(path).unwrap();

            let stream1 = archive.register_stream("stream1");
            let stream2 = archive.register_stream("stream2");

            archive.add_part(stream1, b"Data1", 1).unwrap();
            archive.add_part(stream2, b"Data2", 2).unwrap();
            archive.add_part(stream1, b"Data3", 3).unwrap();

            archive.close().unwrap();
        }

        // Read
        {
            let mut archive = Archive::new_reader();
            archive.open(path).unwrap();

            let stream1 = archive.get_stream_id("stream1").unwrap();
            let stream2 = archive.get_stream_id("stream2").unwrap();

            assert_eq!(archive.get_num_parts(stream1), 2);
            assert_eq!(archive.get_num_parts(stream2), 1);

            let (data, meta) = archive.get_part_by_id(stream1, 0).unwrap();
            assert_eq!(data, b"Data1");
            assert_eq!(meta, 1);

            let (data, meta) = archive.get_part_by_id(stream2, 0).unwrap();
            assert_eq!(data, b"Data2");
            assert_eq!(meta, 2);
        }

        fs::remove_file(path).unwrap();
    }
}
